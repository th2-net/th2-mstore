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

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.*;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.google.protobuf.GeneratedMessageV3;
import io.prometheus.client.Histogram;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.exactpro.th2.common.message.MessageUtils.toTimestamp;
import static com.exactpro.th2.common.util.StorageUtils.toInstant;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.BinaryOperator.maxBy;
import static org.apache.commons.lang3.builder.ToStringStyle.NO_CLASS_NAME_STYLE;

public abstract class AbstractMessageStore<T extends GeneratedMessageV3, M extends GeneratedMessageV3> implements AutoCloseable{
    private static final Logger logger = LoggerFactory.getLogger(AbstractMessageStore.class);

    protected final CradleStorage cradleStorage;
    private final ScheduledExecutorService drainExecutor = Executors.newSingleThreadScheduledExecutor();
    private final Map<SessionKey, SessionData> sessions = new ConcurrentHashMap<>();
    private final Map<String, SessionBatchHolder> sessionHolder = new ConcurrentHashMap<>();
    private final Configuration configuration;
    private volatile ScheduledFuture<?> drainFuture;
    private final MessageRouter<T> router;
    private SubscriberMonitor monitor;
    private final Persistor<StoredGroupMessageBatch> persistor;
    private final MessageProcessorMetrics metrics;

    public AbstractMessageStore(
            MessageRouter<T> router,
            @NotNull CradleStorage cradleStorage,
            @NotNull Persistor<StoredGroupMessageBatch> persistor,
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
            monitor = router.subscribeAll((tag, delivery) -> {
                try {
                    handle(delivery);
                } catch (Exception e) {
                    logger.warn("Can not handle delivery from consumer = {}", tag, e);
                }
            }, getAttributes());
            if (monitor != null) {
                logger.info("RabbitMQ subscribing was successful");
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


    public void handle(T messageBatch) {
        try {
            List<M> messages = getMessages(messageBatch);
            if (messages.isEmpty()) {
                if (logger.isWarnEnabled())
                    logger.warn("Empty batch has been received {}", shortDebugString(messageBatch));
                return;
            }
            verifyBatch(messages);
            String sessionGroup = null;
            for (M message : messages) {
                MessageOrderingProperties sequenceToTimestamp = extractOrderingProperties(message);
                SessionKey sessionKey = createSessionKey(message);

                sessionGroup = sessionKey.sessionGroup;
                SessionData sessionData = sessions.computeIfAbsent(sessionKey, k -> new SessionData());

                sessionData.getAndUpdateOrderingProperties(sequenceToTimestamp).getSequence();
            }

            storeMessages(messages, sessionGroup);
        } catch (Exception ex) {
            if (logger.isErrorEnabled()) {
                logger.error("Cannot handle the batch of type {} message id {}", messageBatch.getClass(), shortDebugString(messageBatch), ex);
            }
        }
    }

    protected void storeMessages(List<M> messagesList, String sessionGroup) throws Exception {
        logger.debug("Process {} messages started", messagesList.size());

        StoredGroupMessageBatch storedGroupMessageBatch = cradleStorage.getObjectsFactory().createGroupMessageBatch(sessionGroup);
        for (M message : messagesList) {
            MessageToStore messageToStore = convert(message);
            storedGroupMessageBatch.addMessage(messageToStore);
        }
        StoredGroupMessageBatch holtBatch;
        SessionBatchHolder holder = sessionHolder.computeIfAbsent(sessionGroup,
                k -> new SessionBatchHolder(sessionGroup, () -> cradleStorage.getObjectsFactory().createGroupMessageBatch(sessionGroup)));
        synchronized (holder) {
            if (holder.add(storedGroupMessageBatch)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Message Batch added to the holder: {}", formatStoredMessageBatch(storedGroupMessageBatch, true));
                }
                return;
            }
            holtBatch = holder.resetAndUpdate(storedGroupMessageBatch);
        }

        if (holtBatch.isEmpty() && logger.isDebugEnabled())
            logger.debug("Holder for '{}' has been concurrently reset. Skip storing",
                    storedGroupMessageBatch.getSessionGroup());
        else
            persist(holtBatch);
    }

    private void persist(StoredGroupMessageBatch batch) {
        Histogram.Timer timer = metrics.startMeasuringPersistenceLatency();
        try {
            persistor.persist(batch);
        } catch (Exception e) {
            logger.error("Exception storing store batch for group {}: {}", batch.getSessionGroup(),
                                    formatStoredMessageBatch(batch, false), e);
        } finally {
            timer.observeDuration();
        }
    }

    public static String formatStoredMessageBatch(StoredGroupMessageBatch storedMessageBatch, boolean full) {
        if (storedMessageBatch.getMessageCount() == 0) {
            return "[]";
        }

        ToStringBuilder builder = new ToStringBuilder(storedMessageBatch, NO_CLASS_NAME_STYLE)
                .append("stream", storedMessageBatch.getSessionGroup())
                .append("count", storedMessageBatch.getMessageCount());
        if (full) {
            builder.append("sequences", storedMessageBatch.getMessages().stream()
                            .map(StoredMessage::getId)
                            .map(StoredMessageId::getIndex)
                            .map(Objects::toString)
                            .collect(Collectors.joining(",", "[", "]")));
        }
        return builder.toString();
    }

    private void verifyBatch(List<M> messages) {
        Map<SessionKey, M> batchSessions = new HashMap<>();
        SessionKey firstSessionKey = null;
        for (int i = 0; i < messages.size(); i++) {
            M message = messages.get(i);
            SessionKey sessionKey = createSessionKey(message);
            if (firstSessionKey == null)
                firstSessionKey = sessionKey;

            if(!firstSessionKey.sessionGroup.equals(sessionKey.sessionGroup)){
                throw new IllegalArgumentException(format(
                        "Delivery contains different session groups. Message [%d] - sequence %d; Message [%d] - sequence %d",
                        i - 1,
                        firstSessionKey,
                        i,
                        sessionKey
                ));
            }
            MessageOrderingProperties orderingProperties = extractOrderingProperties(message);
            MessageOrderingProperties lastOrderingProperties;
            if (batchSessions.containsKey(sessionKey)) {
                lastOrderingProperties = extractOrderingProperties(batchSessions.get(sessionKey));
            } else if (sessions.containsKey(sessionKey)){
                lastOrderingProperties = sessions.get(sessionKey).getLastOrderingProperties();
            } else {
                lastOrderingProperties = loadLastOrderingProperties(sessionKey);
            }

            verifyOrderingProperties(i, lastOrderingProperties, orderingProperties);
            batchSessions.put(sessionKey, message);
        }
    }
    private MessageOrderingProperties loadLastOrderingProperties(SessionKey sessionKey){
        long lastMessageSequence;
        try {
            lastMessageSequence = cradleStorage.getLastMessageIndex(sessionKey.session, sessionKey.direction);
        } catch (IOException e) {
            logger.error("Couldn't get sequence of last message from cradle: {}", e.getMessage());
            return MessageOrderingProperties.MIN_VALUE;
        }
        Instant lastMessageTimestamp;
        StoredMessageId storedMessageId = new StoredMessageId(sessionKey.session, sessionKey.direction, lastMessageSequence);
        try {
            StoredMessage message = cradleStorage.getMessage(storedMessageId);
            if (message != null)
                lastMessageTimestamp = message.getTimestamp();
            else
                return MessageOrderingProperties.MIN_VALUE;
        } catch (IOException e) {
            logger.error("Couldn't get timestamp of last message from cradle: {}", e.getMessage());
            return MessageOrderingProperties.MIN_VALUE;
        }
        return new MessageOrderingProperties(lastMessageSequence, toTimestamp(lastMessageTimestamp));
    }

    private void verifyOrderingProperties(
            int messageIndex,
            MessageOrderingProperties previous,
            MessageOrderingProperties current
    ) {
        if (current.sequenceIsLessOrEquals(previous)) {
            throw new IllegalArgumentException(format(
                    "Delivery contains unordered messages. Message [%d] - sequence %d; Message [%d] - sequence %d",
                    messageIndex - 1,
                    previous.getSequence(),
                    messageIndex,
                    current.getSequence()
            ));
        }
        if (current.timestampIsLess(previous)) {
            throw new IllegalArgumentException(format(
                    "Delivery contains unordered messages. Message [%d] - timestamp %s; Message [%d] - timestamp %s",
                    messageIndex - 1,
                    toInstant(previous.getTimestamp()),
                    messageIndex,
                    toInstant(current.getTimestamp())
            ));
        }
    }

    private void drainByScheduler() {
        logger.debug("Start storing batches by scheduler");
        drain(false);
        logger.debug("Stop storing batches by scheduler");
    }

    private void drain(boolean force) {
        sessionHolder.forEach((key, holder) -> drainHolder(holder, force));
    }

    private void drainHolder(SessionBatchHolder holder, boolean force) {
        String group = holder.getGroup();
        logger.trace("Drain holder for group {}; force: {}", group, force);
        StoredGroupMessageBatch batch;
        synchronized (holder) {
            if (!force && !holder.isReadyToReset(configuration.getDrainInterval())) {
                return;
            }
            batch = holder.reset();
        }
        if (batch.isEmpty()) {
            logger.debug("Holder for group: '{}' has been concurrently reset. Skip storing by scheduler", group);
            return;
        }
        persist(batch);
    }

    protected abstract String[] getAttributes();

    protected abstract List<M> getMessages(T delivery);

    protected abstract MessageToStore convert(M originalMessage);

    protected abstract MessageOrderingProperties extractOrderingProperties(M message);

    protected abstract SessionKey createSessionKey(M message);

    protected abstract String shortDebugString(T batch);

    protected static class SessionKey {
        final String session;
        final String sessionGroup;
        final Direction direction;

        public SessionKey(String session, Direction direction, String sessionGroup) {
            this.session = requireNonNull(session, "'session' parameter");
            this.direction = requireNonNull(direction, "'direction' parameter");
            this.sessionGroup = requireNonNull(sessionGroup, "'group' parameter");
        }

        @Override
        public boolean equals(Object other) {
            if (this == other)
                return true;
            if (!(other instanceof SessionKey))
                return false;
            SessionKey that = (SessionKey)other;
            return Objects.equals(this.session, that.session)
                    && Objects.equals(this.sessionGroup, that.sessionGroup)
                    && Objects.equals(this.direction, that.direction);
        }

        @Override
        public int hashCode() {
            return Objects.hash(session, direction, sessionGroup);
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, NO_CLASS_NAME_STYLE)
                    .append("session", session)
                    .append("direction", direction)
                    .append("group", sessionGroup)
                    .toString();
        }
    }

    private static class SessionData {
        private final AtomicReference<MessageOrderingProperties> lastOrderingProperties =
                new AtomicReference<>(MessageOrderingProperties.MIN_VALUE);


        SessionData() {
        }

        public MessageOrderingProperties getAndUpdateOrderingProperties(MessageOrderingProperties orderingProperties) {
            return lastOrderingProperties.getAndAccumulate(orderingProperties, maxBy(MessageOrderingProperties.COMPARATOR));
        }

        public MessageOrderingProperties getLastOrderingProperties() {
            return lastOrderingProperties.get();
        }
    }
}
