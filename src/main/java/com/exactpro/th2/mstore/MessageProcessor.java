/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.mstore;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.message.*;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback.Confirmation;
import io.prometheus.client.Histogram;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.util.Objects.requireNonNull;

public class MessageProcessor implements AutoCloseable  {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessor.class);
    private static final String[] ATTRIBUTES = {QueueAttribute.SUBSCRIBE.getValue(), QueueAttribute.RAW.getValue()};

    protected final CradleStorage cradleStorage;
    private final ScheduledExecutorService drainExecutor = Executors.newSingleThreadScheduledExecutor();
    private final Map<SessionKey, SessionData> sessions = new ConcurrentHashMap<>();
    private final Map<String, BatchConsolidator> batchCaches = new ConcurrentHashMap<>();
    private final Configuration configuration;
    private volatile ScheduledFuture<?> drainFuture;
    private final MessageRouter<RawMessageBatch> router;
    private SubscriberMonitor monitor;
    private final Persistor<GroupedMessageBatchToStore> persistor;
    private final MessageProcessorMetrics metrics;

    public MessageProcessor(
            @NotNull MessageRouter<RawMessageBatch> router,
            @NotNull CradleStorage cradleStorage,
            @NotNull Persistor<GroupedMessageBatchToStore> persistor,
            @NotNull Configuration configuration
    ) {
        this.router = requireNonNull(router, "Message router can't be null");
        this.cradleStorage = requireNonNull(cradleStorage, "Cradle storage can't be null");
        this.persistor = Objects.requireNonNull(persistor, "Persistor can't be null");
        this.configuration = Objects.requireNonNull(configuration, "'Configuration' parameter");
        this.metrics = new MessageProcessorMetrics();
    }

    public void start() {
        if (monitor == null) {

            monitor = router.subscribeAllWithManualAck(this::process, MessageProcessor.ATTRIBUTES);
            if (monitor != null) {
                logger.info("RabbitMQ subscription was successful");
            } else {
                logger.error("Can not find queues for subscribe");
                throw new RuntimeException("Can not find queues for subscriber");
            }
        }
        drainFuture = drainExecutor.scheduleAtFixedRate(this::drainByScheduler,
                                                        configuration.getDrainInterval(),
                                                        configuration.getDrainInterval(),
                                                        TimeUnit.MILLISECONDS);
        logger.info("Drain scheduler is started");
    }


    @Override
    public void close() {
        if (monitor != null) {
            try {
                monitor.unsubscribe();
            } catch (Exception e) {
                logger.error("Can not unsubscribe from queues", e);
            }
        }
        try {
            ScheduledFuture<?> future = this.drainFuture;
            if (future != null) {
                this.drainFuture = null;
                future.cancel(false);
            }
        } catch (Exception ex) {
            logger.error("Cannot cancel drain task", ex);
        }

        try {
            drain(true);
        } catch (Exception ex) {
            logger.error("Cannot drain left batches during shutdown", ex);
        }

        try {
            drainExecutor.shutdown();
            if (!drainExecutor.awaitTermination(configuration.getTerminationTimeout(), TimeUnit.MILLISECONDS)) {
                logger.warn("Drain executor was not terminated during {} millis. Call force shutdown", configuration.getTerminationTimeout());
                List<Runnable> leftTasks = drainExecutor.shutdownNow();
                if (!leftTasks.isEmpty()) {
                    logger.warn("{} tasks left in the queue", leftTasks.size());
                }
            }
        } catch (Exception ex) {
            logger.error("Cannot gracefully shutdown drain executor", ex);
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }


    void process(DeliveryMetadata deliveryMetadata, RawMessageBatch messageBatch, Confirmation confirmation) {
        try {
            List<RawMessage> messages = messageBatch.getMessagesList();
            if (messages.isEmpty()) {
                if (logger.isWarnEnabled())
                    logger.warn("Received empty batch {}", shortDebugString(messageBatch));
                confirm(confirmation);
                return;
            }

            if (!deliveryMetadata.isRedelivered())
                verifyBatch(messages);

            String group = null;
            for (RawMessage message: messages) {
                SessionKey sessionKey = createSessionKey(message);
                group = sessionKey.sessionGroup;
                long sequence = extractSequence(message);
                SessionData sessionData = sessions.computeIfAbsent(sessionKey, k -> new SessionData());

                sessionData.getAndUpdateSequence(sequence);
            }

            if (deliveryMetadata.isRedelivered()) {
                persist(new ConsolidatedBatch(toCradleBatch(group, messages), confirmation));
            } else {
                storeMessages(group, messages, confirmation);
            }
        } catch (Exception ex) {
            logger.error("Cannot handle the batch of type {}, rejecting", messageBatch.getClass(), ex);
            reject(confirmation);
        }
    }


    private static void confirm(Confirmation confirmation) {
        try {
            confirmation.confirm();
        } catch (Exception e) {
            logger.error("Exception confirming message", e);
        }
    }


    private static void reject(Confirmation confirmation) {
        try {
            confirmation.reject();
        } catch (Exception e) {
            logger.error("Exception rejecting message", e);
        }
    }


    private GroupedMessageBatchToStore toCradleBatch(String group, List<RawMessage> messagesList) throws CradleStorageException {
        GroupedMessageBatchToStore batch = cradleStorage.getEntitiesFactory().groupedMessageBatch(group);
        for (RawMessage message : messagesList) {
            MessageToStore messageToStore = ProtoUtil.toCradleMessage(message);
            batch.addMessage(messageToStore);
        }
        return batch;
    }


    private void storeMessages(String group, List<RawMessage> messagesList, Confirmation confirmation) throws Exception {
        logger.debug("Process {} messages started", messagesList.size());

        GroupedMessageBatchToStore batch = toCradleBatch(group, messagesList);
        BatchConsolidator consolidator = batchCaches.computeIfAbsent(group,
                k -> new BatchConsolidator(() -> cradleStorage.getEntitiesFactory().groupedMessageBatch(group)));

        ConsolidatedBatch consolidatedBatch;
        synchronized (consolidator) {
            if (consolidator.add(batch, confirmation)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Message Batch added to the cache: {}", formatMessageBatchToStore(batch, true));
                }
                return;
            }
            consolidatedBatch = consolidator.resetAndUpdate(batch, confirmation);
        }

        if (consolidatedBatch.batch.isEmpty())
            logger.debug("Batch cache for group \"{}\" has been concurrently reset. Skip storing", group);
        else
            persist(consolidatedBatch);
    }


    private String formatMessageBatchToStore(GroupedMessageBatchToStore batch, boolean full) {
        ToStringBuilder builder = new ToStringBuilder(batch, ToStringStyle.NO_CLASS_NAME_STYLE)
                .append("book name", batch.getBookId().getName())
                .append("session group", batch.getGroup());
        if (full) {
            builder.append("size", batch.getBatchSize())
                    .append("count", batch.getMessageCount())
                    .append("message sequences", batch.getMessages().stream()
                            .map(StoredMessage::getId)
                            .map(StoredMessageId::getSequence)
                            .map(Objects::toString)
                            .collect(Collectors.joining(",", "[", "]")));
        }
        return builder.toString();
    }


    private void verifyBatch(List<RawMessage> messages) {
        HashMap<SessionKey, SessionData> localCache = new HashMap<>();
        SessionKey lastKey = null;
        long prevSequence = Long.MIN_VALUE;
        for (int i = 0; i < messages.size(); i++) {
            RawMessage message = messages.get(i);
            SessionKey sessionKey = createSessionKey(message);
            long currentSeq = extractSequence(message);

            if (localCache.containsKey(sessionKey)) {
                prevSequence = localCache.get(sessionKey).lastSequence.get();
            } else if(sessions.containsKey(sessionKey)) {
                prevSequence = sessions.get(sessionKey).lastSequence.get();
            } else {
                prevSequence = getLastMessageSequence(sessionKey);
            }

            if (lastKey == null) {
                lastKey = sessionKey;
            } else {
                if (!lastKey.sessionGroup.equals(sessionKey.sessionGroup)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Delivery contains different session groups. Message [%d] - session %s; Message [%d] - session %s",
                                    i - 1, lastKey, i, sessionKey
                            )
                    );
                }
            }

            if (prevSequence >= currentSeq) {
                throw new IllegalArgumentException(
                        String.format(
                                "Delivery contains unordered messages. Message [%d] - seqN %d; Message [%d] - seqN %d",
                                i - 1, prevSequence, i, currentSeq
                        )
                );
            }
            SessionData sessionData = new SessionData();
            sessionData.getAndUpdateSequence(currentSeq);
            localCache.put(sessionKey, sessionData);
        }
    }


    private long getLastMessageSequence(SessionKey sessionKey) {
        long lastMessageSequence = Long.MIN_VALUE;
        try {
            lastMessageSequence = cradleStorage.getLastSequence(sessionKey.sessionAlias, sessionKey.direction, new BookId(sessionKey.bookName));
        } catch (Exception e) {
            logger.error("Couldn't get sequence of last message from cradle: {}", e.getMessage());
        }
        return lastMessageSequence;
    }


    private void drainByScheduler() {
        logger.debug("Starting scheduled cache drain");
        drain(false);
        logger.debug("Scheduled cache drain ended");
    }

    private void drain(boolean force) {
        batchCaches.forEach((group, consolidator) -> {
            logger.trace("Draining cache for group \"{}\" (forced={})", group, force);
            ConsolidatedBatch data;
            synchronized (consolidator) {
                if (!force && ((consolidator.ageInMillis() < configuration.getDrainInterval()) || consolidator.isEmpty()))
                    return;
                data = consolidator.reset();
            }
            if (data.batch.isEmpty())
                return;
            persist(data);
        });
    }


    private void persist(ConsolidatedBatch data) {
        GroupedMessageBatchToStore batch = data.batch;
        Histogram.Timer timer = metrics.startMeasuringPersistenceLatency();
        try {
            persistor.persist(batch, new Callback<>() {
                @Override
                public void onSuccess(GroupedMessageBatchToStore batch) {
                    data.confirmations.forEach(MessageProcessor::confirm);
                }

                @Override
                public void onFail(GroupedMessageBatchToStore batch) {
                    data.confirmations.forEach(MessageProcessor::reject);
                }
            });
        } catch (Exception e) {
            logger.error("Exception storing batch for group \"{}\": {}", batch.getGroup(),
                            formatMessageBatchToStore(batch, false), e);
            data.confirmations.forEach(MessageProcessor::reject);
        } finally {
            timer.observeDuration();
        }
    }


    private SessionKey createSessionKey(RawMessage message) {
        return new SessionKey(message.getMetadata().getId());
    }

    private long extractSequence(RawMessage message) {
        return message.getMetadata().getId().getSequence();
    }

    protected static class SessionKey {
        public final String bookName;
        public final String sessionAlias;
        public final String sessionGroup;
        public final Direction direction;

        public SessionKey(MessageID messageID) {
            this.sessionAlias = Objects.requireNonNull(messageID.getConnectionId().getSessionAlias(), "'Session alias' parameter");
            this.direction = Objects.requireNonNull(toCradleDirection(messageID.getDirection()), "'Direction' parameter");
            this.bookName = Objects.requireNonNull(messageID.getBookName(), "'Book name' parameter");

            String group = messageID.getConnectionId().getSessionGroup();
            this.sessionGroup = (group == null || group.isEmpty()) ? this.sessionAlias : group;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (!(other instanceof SessionKey))
                return false;
            SessionKey that = (SessionKey) other;
            return Objects.equals(sessionAlias, that.sessionAlias) &&
                    Objects.equals(sessionGroup, that.sessionGroup) &&
                    Objects.equals(direction, that.direction) &&
                    Objects.equals(bookName, that.bookName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sessionAlias, sessionGroup, direction, bookName);
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
                    .append("sessionAlias", sessionAlias)
                    .append("sessionGroup", sessionGroup)
                    .append("direction", direction)
                    .append("bookName", bookName)
                    .toString();
        }
    }

    static class SessionData {
        private final AtomicLong lastSequence = new AtomicLong(Long.MIN_VALUE);

        SessionData() {
        }

        public long getAndUpdateSequence(long newLastSeq) {
            return lastSequence.getAndAccumulate(newLastSeq, Math::max);
        }
    }
}
