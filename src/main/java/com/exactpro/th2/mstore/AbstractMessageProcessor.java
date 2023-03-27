/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.Order;
import com.exactpro.cradle.filters.FilterForLess;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.MessageFilter;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.resultset.CradleResultSet;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback.Confirmation;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import io.prometheus.client.Histogram;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.exactpro.th2.common.message.MessageUtils.toTimestamp;
import static com.exactpro.th2.common.util.StorageUtils.toInstant;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public abstract class AbstractMessageProcessor implements AutoCloseable  {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMessageProcessor.class);
    protected static final String[] ATTRIBUTES = {QueueAttribute.SUBSCRIBE.getValue(), QueueAttribute.RAW.getValue()};

    protected final CradleStorage cradleStorage;
    private final ScheduledExecutorService drainExecutor = Executors.newSingleThreadScheduledExecutor();
    protected final Map<SessionKey, SessionData> sessions = new ConcurrentHashMap<>();
    private final Map<String, BatchConsolidator> batchCaches = new ConcurrentHashMap<>();
    private final Configuration configuration;
    private volatile ScheduledFuture<?> drainFuture;
    private final Persistor<GroupedMessageBatchToStore> persistor;
    private final MessageProcessorMetrics metrics;
    private final ManualDrainTrigger manualDrain;

    public AbstractMessageProcessor(
            @NotNull CradleStorage cradleStorage,
            @NotNull Persistor<GroupedMessageBatchToStore> persistor,
            @NotNull Configuration configuration,
            @NotNull Integer prefetchCount
    ) {
        this.cradleStorage = requireNonNull(cradleStorage, "Cradle storage can't be null");
        this.persistor = Objects.requireNonNull(persistor, "Persistor can't be null");
        this.configuration = Objects.requireNonNull(configuration, "'Configuration' parameter");
        this.metrics = new MessageProcessorMetrics();

        this.manualDrain = new ManualDrainTrigger(drainExecutor, (int) Math.round(prefetchCount * configuration.getPrefetchRatioToDrain()));
    }

    public void start() {
        logger.info("Rebatching is {}", (configuration.isRebatching() ? "on" : "off"));
        if (configuration.isRebatching()) {
            drainFuture = drainExecutor.scheduleAtFixedRate(this::scheduledDrain,
                                                            configuration.getDrainInterval(),
                                                            configuration.getDrainInterval(),
                                                            TimeUnit.MILLISECONDS);
            logger.info("Drain scheduler is started");
        }
    }


    @Override
    public void close() {
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


    protected void storeMessages(GroupedMessageBatchToStore batch, Confirmation confirmation) throws Exception {
        logger.trace("Process {} messages started", batch.getMessageCount());

        ConsolidatedBatch consolidatedBatch;
        if (configuration.isRebatching()) {
            BatchConsolidator consolidator = batchCaches.computeIfAbsent(batch.getGroup(),
                    k -> new BatchConsolidator(() -> cradleStorage.getEntitiesFactory().groupedMessageBatch(batch.getGroup()), configuration.getMaxBatchSize()));

            synchronized (consolidator) {
                if (consolidator.add(batch, confirmation)) {
                    manualDrain.registerMessage();
                    if (logger.isTraceEnabled()) {
                        logger.trace("Message Batch added to the cache: {}", formatMessageBatchToStore(batch, true));
                    }

                    manualDrain.runConditionally(this::manualDrain);
                    return;
                }

                consolidatedBatch = consolidator.resetAndUpdate(batch, confirmation);

                manualDrain.unregisterMessages(consolidatedBatch.confirmations.size());
                manualDrain.registerMessage();
                manualDrain.runConditionally(this::manualDrain);
            }
        } else {
            consolidatedBatch = new ConsolidatedBatch(batch, confirmation);
        }

        if (consolidatedBatch.batch.isEmpty())
            logger.debug("Batch cache for group \"{}\" has been concurrently reset. Skip storing", batch.getGroup());
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

    protected void verifyOrderingProperties(
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

    protected MessageOrderingProperties loadLastOrderingProperties(SessionKey sessionKey){
        long lastMessageSequence;
        Instant lastMessageTimestamp;
        try {
            MessageFilter messageFilter = new MessageFilter(new BookId(sessionKey.bookName), sessionKey.sessionAlias, sessionKey.direction);
            messageFilter.setTimestampTo(FilterForLess.forLess(Instant.now()));
            messageFilter.setOrder(Order.REVERSE);
            messageFilter.setLimit(1);
            CradleResultSet<StoredMessage> res = cradleStorage.getMessages(messageFilter);

            if (res == null) {
                return MessageOrderingProperties.MIN_VALUE;
            }

            StoredMessage message = res.next();

            if (message == null) {
                return MessageOrderingProperties.MIN_VALUE;
            }

            lastMessageSequence = message.getSequence();
            lastMessageTimestamp = message.getTimestamp();
        } catch (CradleStorageException | IOException | NoSuchElementException e) {
            logger.trace("Couldn't get last message from cradle: {}", e.getMessage());
            return MessageOrderingProperties.MIN_VALUE;
        }

        return new MessageOrderingProperties(lastMessageSequence, toTimestamp(lastMessageTimestamp));
    }

    private void scheduledDrain() {
        logger.trace("Starting scheduled cache drain");
        drain(false);
        logger.trace("Scheduled cache drain ended");
    }

    private void manualDrain() {
        logger.trace("Starting manual cache drain");
        drain(true);
        manualDrain.completeDraining();
        logger.trace("Manual cache drain ended");
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
            manualDrain.unregisterMessages(data.confirmations.size());
            if (data.batch.isEmpty())
                return;
            persist(data);
        });
    }


    protected void persist(ConsolidatedBatch data) {
        GroupedMessageBatchToStore batch = data.batch;
        try (Histogram.Timer ignored = metrics.startMeasuringPersistenceLatency()) {
            persistor.persist(batch, new Callback<>() {
                @Override
                public void onSuccess(GroupedMessageBatchToStore batch) {
                    data.confirmations.forEach(AbstractMessageProcessor::confirm);
                }

                @Override
                public void onFail(GroupedMessageBatchToStore batch) {
                    data.confirmations.forEach(AbstractMessageProcessor::reject);
                }
            });
        } catch (Exception e) {
            logger.error("Exception storing batch for group \"{}\": {}", batch.getGroup(),
                            formatMessageBatchToStore(batch, false), e);
            data.confirmations.forEach(AbstractMessageProcessor::reject);
        }
    }

    //FIXME: com.exactpro.th2.mstore.MessageProcessor.verifyBatch() 22,468 ms (9.3%)
    //static hash
    protected static class SessionKey {
        public final String bookName;
        public final String sessionAlias;
        public final String sessionGroup;
        public final Direction direction;

        public SessionKey(String bookName, String sessionGroup, String sessionAlias, Direction direction) {
            this.sessionAlias = Objects.requireNonNull(sessionAlias, "'Session alias' parameter");
            this.direction = Objects.requireNonNull(direction, "'Direction' parameter");
            this.bookName = Objects.requireNonNull(bookName, "'Book name' parameter");
            this.sessionGroup = (sessionGroup == null || sessionGroup.isEmpty()) ? sessionAlias : sessionGroup;
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
        private final MessageOrderingProperties lastOrderingProperties;


        SessionData(MessageOrderingProperties orderingProperties) {
            lastOrderingProperties = orderingProperties;
        }

        public MessageOrderingProperties getLastOrderingProperties() {
            return lastOrderingProperties;
        }
    }
}
