/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.messages.GroupedMessageFilter;
import com.exactpro.cradle.messages.MessageFilter;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredGroupedMessageBatch;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.resultset.CradleResultSet;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.grpc.RawMessageBatchOrBuilder;
import com.exactpro.th2.common.grpc.RawMessageOrBuilder;
import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback.Confirmation;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import io.prometheus.client.Histogram.Timer;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.exactpro.th2.common.message.MessageUtils.toTimestamp;
import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static com.exactpro.th2.common.util.StorageUtils.toInstant;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.BinaryOperator.maxBy;

public class MessageProcessor implements AutoCloseable  {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProcessor.class);
    private static final String[] ATTRIBUTES = {QueueAttribute.SUBSCRIBE.getValue(), QueueAttribute.RAW.getValue()};

    protected final CradleStorage cradleStorage;
    private final ScheduledExecutorService drainExecutor = Executors.newSingleThreadScheduledExecutor();
    private final Map<SessionKey, SessionData> sessions = new ConcurrentHashMap<>();
    private final Map<GroupKey, Instant> groups = new ConcurrentHashMap<>();
    private final Map<GroupKey, BatchConsolidator> batchCaches = new ConcurrentHashMap<>();

    private final Configuration configuration;
    private volatile @Nullable ScheduledFuture<?> drainFuture;
    private final MessageRouter<RawMessageBatch> router;
    private SubscriberMonitor monitor;
    private final Persistor<GroupedMessageBatchToStore> persistor;
    private final MessageProcessorMetrics metrics;
    private final ManualDrainTrigger manualDrain;

    public MessageProcessor(
            @NotNull MessageRouter<RawMessageBatch> router,
            @NotNull CradleStorage cradleStorage,
            @NotNull Persistor<GroupedMessageBatchToStore> persistor,
            @NotNull Configuration configuration,
            @NotNull Integer prefetchCount
    ) {
        this.router = requireNonNull(router, "Message router can't be null");
        this.cradleStorage = requireNonNull(cradleStorage, "Cradle storage can't be null");
        this.persistor = requireNonNull(persistor, "Persistor can't be null");
        this.configuration = requireNonNull(configuration, "'Configuration' parameter");
        this.metrics = new MessageProcessorMetrics();

        this.manualDrain = new ManualDrainTrigger(drainExecutor, (int) Math.round(prefetchCount * configuration.getPrefetchRatioToDrain()));
    }

    public void start() {
        if (monitor == null) {

            monitor = router.subscribeAllWithManualAck(this::process, ATTRIBUTES);
            if (monitor == null) {
                LOGGER.error("Can not find queues for subscribe");
                throw new RuntimeException("Can not find queues for subscriber");
            }
            LOGGER.info("RabbitMQ subscription was successful");
        }

        LOGGER.info("Rebatching is {}", (configuration.isRebatching() ? "on" : "off"));
        if (configuration.isRebatching()) {
            drainFuture = drainExecutor.scheduleAtFixedRate(this::scheduledDrain,
                                                            configuration.getDrainInterval(),
                                                            configuration.getDrainInterval(),
                                                            TimeUnit.MILLISECONDS);
            LOGGER.info("Drain scheduler is started");
        }
    }


    @Override
    public void close() {
        if (monitor != null) {
            try {
                monitor.unsubscribe();
            } catch (IOException | RuntimeException e) {
                LOGGER.error("Can not unsubscribe from queues", e);
            }
        }
        try {
            ScheduledFuture<?> future = drainFuture;
            if (future != null) {
                this.drainFuture = null;
                future.cancel(false);
            }
        } catch (RuntimeException ex) {
            LOGGER.error("Cannot cancel drain task", ex);
        }

        try {
            drain(true);
        } catch (RuntimeException ex) {
            LOGGER.error("Cannot drain left batches during shutdown", ex);
        }

        try {
            drainExecutor.shutdown();
            if (!drainExecutor.awaitTermination(configuration.getTerminationTimeout(), TimeUnit.MILLISECONDS)) {
                LOGGER.warn("Drain executor was not terminated during {} millis. Call force shutdown", configuration.getTerminationTimeout());
                List<Runnable> leftTasks = drainExecutor.shutdownNow();
                if (!leftTasks.isEmpty()) {
                    LOGGER.warn("{} tasks left in the queue", leftTasks.size());
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error("Cannot gracefully shutdown drain executor", e);
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            LOGGER.error("Cannot gracefully shutdown drain executor", e);
        }
    }


    void process(DeliveryMetadata deliveryMetadata, RawMessageBatchOrBuilder messageBatch, Confirmation confirmation) {
        try {
            List<RawMessage> messages = messageBatch.getMessagesList();
            if (messages.isEmpty()) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Received empty batch {}", shortDebugString(messageBatch));
                }
                confirm(confirmation);
                return;
            }

            var firstMessage = messages.get(0);
            GroupKey groupKey = new GroupKey(firstMessage.getMetadata().getId());

            if (!deliveryMetadata.isRedelivered()) {
                verifyBatch(groupKey, messages);
            }

            GroupedMessageBatchToStore groupedBatchToStore = toCradleBatch(groupKey.group, messages);

            for (RawMessage message: messages) {
                SessionKey sessionKey = createSessionKey(message);
                MessageOrderingProperties sequenceToTimestamp = extractOrderingProperties(message);
                SessionData sessionData = sessions.computeIfAbsent(sessionKey, key -> new SessionData());

                sessionData.getAndUpdateOrderingProperties(sequenceToTimestamp);
            }
            groups.put(groupKey, groupedBatchToStore.getLastTimestamp());

            if (deliveryMetadata.isRedelivered()) {
                persist(new ConsolidatedBatch(groupedBatchToStore, confirmation));
            } else {
                storeMessages(groupedBatchToStore, groupKey, confirmation);
            }
        } catch (CradleStorageException | RuntimeException ex) {
            LOGGER.error("Cannot handle the batch of type {}, rejecting", messageBatch.getClass(), ex);
            reject(confirmation);
        }
    }

    protected MessageOrderingProperties extractOrderingProperties(RawMessageOrBuilder message) {
        return new MessageOrderingProperties(
                message.getMetadata().getId().getSequence(),
                message.getMetadata().getId().getTimestamp()
        );
    }

    private static void confirm(Confirmation confirmation) {
        try {
            confirmation.confirm();
        } catch (IOException | RuntimeException e) {
            LOGGER.error("Exception confirming message", e);
        }
    }


    private static void reject(Confirmation confirmation) {
        try {
            confirmation.reject();
        } catch (IOException | RuntimeException e) {
            LOGGER.error("Exception rejecting message", e);
        }
    }


    private GroupedMessageBatchToStore toCradleBatch(String group, Iterable<RawMessage> messagesList) throws CradleStorageException {
        GroupedMessageBatchToStore batch = cradleStorage.getEntitiesFactory().groupedMessageBatch(group);
        for (RawMessage message : messagesList) {
            MessageToStore messageToStore = ProtoUtil.toCradleMessage(message);
            batch.addMessage(messageToStore);
        }
        return batch;
    }


    private void storeMessages(GroupedMessageBatchToStore batch, GroupKey groupKey, Confirmation confirmation) throws CradleStorageException {
        LOGGER.trace("Process {} messages started", batch.getMessageCount());

        ConsolidatedBatch consolidatedBatch;
        if (configuration.isRebatching()) {
            BatchConsolidator consolidator = batchCaches.computeIfAbsent(groupKey,
                    key -> new BatchConsolidator(() -> cradleStorage.getEntitiesFactory().groupedMessageBatch(batch.getGroup()), configuration.getMaxBatchSize()));

            synchronized (consolidator) {
                if (consolidator.add(batch, confirmation)) {
                    manualDrain.registerMessage();
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Message Batch added to the cache: {}", formatMessageBatchToStore(batch, true));
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

        if (consolidatedBatch.batch.isEmpty()) {
            LOGGER.debug("Batch cache for group \"{}\" has been concurrently reset. Skip storing", batch.getGroup());
        } else {
            persist(consolidatedBatch);
        }
    }


    private static String formatMessageBatchToStore(GroupedMessageBatchToStore batch, boolean full) {
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


    private void verifyBatch(GroupKey groupKey, List<RawMessage> messages) {
        var firstMessage = messages.get(0);

        var bookId = new BookId(firstMessage.getMetadata().getId().getBookName());

        Instant ts = groups.get(groupKey);
        if (ts == null) {
            ts = loadLastMessageTimestamp(bookId, groupKey.group);
        }

        Map<SessionKey, SessionData> localCache = new HashMap<>();
        SessionKey firstSessionKey = null;
        for (int i = 0; i < messages.size(); i++) {
            RawMessage message = messages.get(i);

            Instant messageTimestamp = toInstant(message.getMetadata().getId().getTimestamp());
            if (ts.isAfter(messageTimestamp)) {
                throw new IllegalArgumentException(format(
                        "Received grouped batch `%s` containing message with timestamp %s, but previously was %s",
                        groupKey.group,
                        messageTimestamp,
                        ts
                ));
            }

            SessionKey sessionKey = createSessionKey(message);
            if (firstSessionKey == null) {
                firstSessionKey = sessionKey;
            }

            if(!firstSessionKey.sessionGroup.equals(sessionKey.sessionGroup)){
                throw new IllegalArgumentException(format(
                        "Delivery contains different session groups. Message [%d] - sequence %s; Message [%d] - sequence %s",
                        i - 1,
                        firstSessionKey,
                        i,
                        sessionKey
                ));
            }

            MessageOrderingProperties orderingProperties = extractOrderingProperties(message);
            MessageOrderingProperties lastOrderingProperties;
            if (localCache.containsKey(sessionKey)) {
                lastOrderingProperties = localCache.get(sessionKey).getLastOrderingProperties();
            }else if (sessions.containsKey(sessionKey)){
                lastOrderingProperties = sessions.get(sessionKey).getLastOrderingProperties();
            } else {
                lastOrderingProperties = loadLastOrderingProperties(sessionKey);
            }

            verifyOrderingProperties(i, lastOrderingProperties, orderingProperties);

            SessionData sessionData = new SessionData();
            sessionData.getAndUpdateOrderingProperties(orderingProperties);
            localCache.put(sessionKey, sessionData);
        }
    }

    private static void verifyOrderingProperties(
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

    MessageOrderingProperties loadLastOrderingProperties(SessionKey sessionKey){
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
            LOGGER.trace("Couldn't get last message from cradle: {}", e.getMessage());
            return MessageOrderingProperties.MIN_VALUE;
        }

        return new MessageOrderingProperties(lastMessageSequence, toTimestamp(lastMessageTimestamp));
    }

    Instant loadLastMessageTimestamp(BookId book, String groupName){
        try {
            GroupedMessageFilter filter = new GroupedMessageFilter(book, groupName);
            filter.setTo(FilterForLess.forLess(Instant.now()));
            filter.setOrder(Order.REVERSE);
            filter.setLimit(1);
            CradleResultSet<StoredGroupedMessageBatch> res = cradleStorage.getGroupedMessageBatches(filter);

            if (res == null) {
                return Instant.MIN;
            }

            StoredGroupedMessageBatch batch = res.next();

            if (batch == null) {
                return Instant.MIN;
            }

            return batch.getLastTimestamp();
        } catch (CradleStorageException | IOException | NoSuchElementException e) {
            LOGGER.trace("Couldn't get last message timestamp for group {}: {}", groupName, e.getMessage());
            return Instant.MIN;
        }
    }


    private void scheduledDrain() {
        LOGGER.trace("Starting scheduled cache drain");
        drain(false);
        LOGGER.trace("Scheduled cache drain ended");
    }

    private void manualDrain() {
        LOGGER.trace("Starting manual cache drain");
        drain(true);
        manualDrain.completeDraining();
        LOGGER.trace("Manual cache drain ended");
    }

    private void drain(boolean force) {
        batchCaches.forEach((group, consolidator) -> {
            LOGGER.trace("Draining cache for group \"{}\" (forced={})", group, force);
            ConsolidatedBatch data;
            synchronized (consolidator) {
                if (!force && ((consolidator.ageInMillis() < configuration.getDrainInterval()) || consolidator.isEmpty())) {
                    return;
                }
                data = consolidator.reset();
            }
            manualDrain.unregisterMessages(data.confirmations.size());
            if (data.batch.isEmpty()) {
                return;
            }
            persist(data);
        });
    }


    private void persist(ConsolidatedBatch data) {
        GroupedMessageBatchToStore batch = data.batch;
        try(Timer ignored = metrics.startMeasuringPersistenceLatency()) {
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
            LOGGER.error("Exception storing batch for group \"{}\": {}", batch.getGroup(),
                            formatMessageBatchToStore(batch, false), e);
            data.confirmations.forEach(MessageProcessor::reject);
        }
    }

    private static SessionKey createSessionKey(RawMessageOrBuilder message) {
        return new SessionKey(message.getMetadata().getId());
    }

    protected static class SessionKey {
        public final String bookName;
        public final String sessionAlias;
        public final String sessionGroup;
        public final Direction direction;

        public SessionKey(MessageID messageID) {
            this.sessionAlias = requireNonNull(messageID.getConnectionId().getSessionAlias(), "'Session alias' parameter");
            this.direction = requireNonNull(toCradleDirection(messageID.getDirection()), "'Direction' parameter");
            this.bookName = requireNonNull(messageID.getBookName(), "'Book name' parameter");

            String group = messageID.getConnectionId().getSessionGroup();
            this.sessionGroup = group.isEmpty() ? sessionAlias : group;
        }

        public SessionKey (String bookName, String sessionAlias, String sessionGroup, Direction direction) {
            this.bookName = bookName;
            this.sessionAlias = sessionAlias;
            this.sessionGroup = sessionGroup;
            this.direction = direction;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof SessionKey)) {
                return false;
            }
            SessionKey that = (SessionKey) other;
            return Objects.equals(sessionAlias, that.sessionAlias) &&
                    Objects.equals(sessionGroup, that.sessionGroup) &&
                    direction == that.direction &&
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

    protected static class GroupKey {
        public final String bookName;
        public final String group;

        public GroupKey(MessageID messageID) {
            this.bookName = requireNonNull(messageID.getBookName(), "'Book name' parameter");
            String sessionGroup = messageID.getConnectionId().getSessionGroup();
            this.group = sessionGroup.isEmpty() ?
                    requireNonNull(messageID.getConnectionId().getSessionAlias(), "'Session alias' parameter") : sessionGroup;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof GroupKey)) {
                return false;
            }
            GroupKey that = (GroupKey) other;
            return  Objects.equals(group, that.group) &&
                    Objects.equals(bookName, that.bookName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(group, bookName);
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
                    .append("group", group)
                    .append("bookName", bookName)
                    .toString();
        }
    }

    static class SessionData {
        private final AtomicReference<MessageOrderingProperties> lastOrderingProperties =
                new AtomicReference<>(MessageOrderingProperties.MIN_VALUE);
        public MessageOrderingProperties getAndUpdateOrderingProperties(MessageOrderingProperties orderingProperties) {
            return lastOrderingProperties.getAndAccumulate(orderingProperties, maxBy(MessageOrderingProperties.COMPARATOR));
        }
        public MessageOrderingProperties getLastOrderingProperties() {
            return lastOrderingProperties.get();
        }
    }
}
