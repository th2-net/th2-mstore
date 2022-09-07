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

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.mstore.cfg.MessageStoreConfiguration;
import com.exactpro.th2.taskutils.FutureTracker;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.exactpro.th2.common.message.MessageUtils.toTimestamp;
import static com.exactpro.th2.common.util.StorageUtils.toInstant;
import static com.exactpro.th2.mstore.SequenceToTimestamp.SEQUENCE_TO_TIMESTAMP_COMPARATOR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.BinaryOperator.maxBy;
import static org.apache.commons.lang3.builder.ToStringStyle.NO_CLASS_NAME_STYLE;

public abstract class AbstractMessageStore<T extends GeneratedMessageV3, M extends GeneratedMessageV3> implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMessageStore.class);

    protected final CradleStorage cradleStorage;
    private final ScheduledExecutorService drainExecutor = Executors.newSingleThreadScheduledExecutor();
    private final Map<SessionKey, SessionData> sessionToHolder = new ConcurrentHashMap<>();
    private final MessageStoreConfiguration configuration;
    private final FutureTracker<Void> futures;
    private volatile ScheduledFuture<?> drainFuture;
    private final MessageRouter<T> router;
    private SubscriberMonitor monitor;

    public AbstractMessageStore(
            @NotNull MessageRouter<T> router,
            @NotNull CradleManager cradleManager,
            @NotNull MessageStoreConfiguration configuration
    ) {
        this.router = requireNonNull(router, "Message router can't be null");
        this.cradleStorage = requireNonNull(cradleManager.getStorage(), "Cradle storage can't be null");
        this.configuration = Objects.requireNonNull(configuration, "'Configuration' parameter");
        this.futures = new FutureTracker<>();
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
        drainFuture = drainExecutor.scheduleAtFixedRate(this::drainByScheduler, configuration.getDrainInterval(), configuration.getDrainInterval(), TimeUnit.MILLISECONDS);
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

        futures.awaitRemaining();
    }


    public final void handle(T messageBatch) {
        try {
            verifyBatch(messageBatch);
            List<M> messages = getMessages(messageBatch);
            if (messages.isEmpty()) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Empty batch has been received {}", shortDebugString(messageBatch));
                }
                return;
            }
            M firstMessage = messages.get(0);
            M lastMessage = messages.get(messages.size() - 1);
            SessionData sessionData = sessionToHolder.computeIfAbsent(
                    createSessionKey(lastMessage),
                    ignore -> new SessionData(cradleStorage.getObjectsFactory()::createMessageBatch)
            );

            SequenceToTimestamp first = extractSequenceToTimestamp(firstMessage);
            SequenceToTimestamp last = extractSequenceToTimestamp(lastMessage);
            SequenceToTimestamp previousBatchLast = sessionData.getAndUpdateLastSequenceToTimestamp(last);
            if (first.sequenceIsLessOrEquals(previousBatchLast)) {
                if (logger.isErrorEnabled()) {
                    logger.error(
                            "Found batch with less or equal sequence. Previous sequence: {}, current batch: {}",
                            previousBatchLast.getSequence(),
                            shortDebugString(messageBatch)
                    );
                }
                return;
            }
            if (first.timestampIsLess(previousBatchLast)) {
                if (logger.isErrorEnabled()) {
                    logger.error(
                            "Found batch with less timestamp. Previous timestamp: {}, current batch: {}",
                            toInstant(previousBatchLast.getTimestamp()),
                            shortDebugString(messageBatch)
                    );
                }
                return;
            }
            storeMessages(messages, sessionData.getBatchHolder());
        } catch (Exception ex) {
            if (logger.isErrorEnabled()) {
                logger.error("Cannot handle the batch of type {} message id {}", messageBatch.getClass(), shortDebugString(messageBatch), ex);
            }
        }
    }

    protected void storeMessages(List<M> messagesList, SessionBatchHolder holder) throws CradleStorageException {
        logger.debug("Process {} messages started", messagesList.size());

        StoredMessageBatch storedMessageBatch = cradleStorage.getObjectsFactory().createMessageBatch();
        for (M message : messagesList) {
            MessageToStore messageToStore = convert(message);
            storedMessageBatch.addMessage(messageToStore);
        }
        StoredMessageBatch holtBatch;
        synchronized (holder) {
            if (holder.add(storedMessageBatch)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Message Batch added to the holder: {}", formatStoredMessageBatch(storedMessageBatch, true));
                }
                return;
            }
            holtBatch = holder.resetAndUpdate(storedMessageBatch);
        }

        if (holtBatch.isEmpty()) {
            logger.debug("Holder for stream: '{}', direction: '{}' has been concurrently reset. Skip storing",
                    storedMessageBatch.getStreamName(), storedMessageBatch.getDirection());
        } else {
            storeBatchAsync(holtBatch);
        }
    }

    private void storeBatchAsync(StoredMessageBatch holtBatch) {
        CompletableFuture<Void> future = store(holtBatch);
        futures.track(future);
        future.whenCompleteAsync((value, exception) -> {
            if (exception == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("{} - batch stored: {}", getClass().getSimpleName(), formatStoredMessageBatch(holtBatch, true));
                }
            } else {
                logger.error("{} - batch storing is failure: {}", getClass().getSimpleName(), formatStoredMessageBatch(holtBatch, true), exception);
            }
        });
    }

    public static String formatStoredMessageBatch(StoredMessageBatch storedMessageBatch, boolean full) {
        if (storedMessageBatch.getMessageCount() == 0) {
            return "[]";
        }

        ToStringBuilder builder = new ToStringBuilder(storedMessageBatch, NO_CLASS_NAME_STYLE)
                .append("stream", storedMessageBatch.getStreamName())
                .append("direction", storedMessageBatch.getId().getDirection())
                .append("batch id", storedMessageBatch.getId().getIndex())
                .append("min timestamp", storedMessageBatch.getFirstMessage().getTimestamp())
                .append("max timestamp", storedMessageBatch.getLastMessage().getTimestamp())
                .append("size", storedMessageBatch.getBatchSize())
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

    /**
     * Checks that the delivery contains all messages related to one session
     * and that each message has sequence number greater than the previous one.
     * @param delivery the delivery received from router
     */
    private void verifyBatch(T delivery) {
        List<M> messages = getMessages(delivery);
        SessionKey previousKey = null;
        if(messages.isEmpty()){
            return;
        }
        previousKey = createSessionKey(messages.get(0));


        SequenceToTimestamp previousSequenceToTimestamp = getLastSequenceToTimeStamp(previousKey);
        for (int i = 0; i < messages.size(); i++) {
            M message = messages.get(i);
            SessionKey sessionKey = createSessionKey(message);
            if (previousKey == null) {
                previousKey = sessionKey;
            } else {
                verifySession(i, previousKey, sessionKey);
            }

            SequenceToTimestamp currentSequenceToTimestamp = extractSequenceToTimestamp(message);
            verifySequenceToTimestamp(i, previousSequenceToTimestamp, currentSequenceToTimestamp);
            previousSequenceToTimestamp = currentSequenceToTimestamp;
        }
    }
    private SequenceToTimestamp getLastSequenceToTimeStamp(SessionKey sessionKey){

        long lastSequence = -1L;
        try {
            lastSequence = cradleStorage.getLastMessageIndex(sessionKey.streamName, sessionKey.direction);
        } catch (IOException e) {
            logger.error("Couldn't get sequence of last message from cradle: {}", e.getMessage());
        }
        Instant lastTimeInstant = Instant.MIN;
        StoredMessageId storedMsgId = new StoredMessageId(sessionKey.streamName, sessionKey.direction, lastSequence);
        try {
            StoredMessage message = cradleStorage.getMessage(storedMsgId);
            if (message != null)
                lastTimeInstant = message.getTimestamp();
        } catch (IOException e) {
            logger.error("Couldn't get timestamp of last message from cradle: {}", e.getMessage());
        }
        return new SequenceToTimestamp(lastSequence, toTimestamp(lastTimeInstant));
    }

    private static void verifySession(int messageIndex, SessionKey previousKey, SessionKey sessionKey) {
        if (!previousKey.equals(sessionKey)) {
            throw new IllegalArgumentException(format(
                    "Delivery contains different sessions. Message [%d] - session %s; Message [%d] - session %s",
                    messageIndex - 1,
                    previousKey,
                    messageIndex,
                    sessionKey
            ));
        }
    }

    private static void verifySequenceToTimestamp(
            int messageIndex,
            SequenceToTimestamp previous,
            SequenceToTimestamp current
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
        sessionToHolder.forEach((key, sessionData) -> drainHolder(key, sessionData.getBatchHolder(), force));
    }

    private void drainHolder(SessionKey key, SessionBatchHolder holder, boolean force) {
        logger.trace("Drain holder for session {}; force: {}", key, force);
        StoredMessageBatch batch;
        synchronized (holder) {
            if (!force && !holder.isReadyToReset(configuration.getDrainInterval())) {
                return;
            }
            batch = holder.reset();
        }
        if (batch.isEmpty()) {
            logger.debug("Holder for stream: '{}', direction: '{}' has been concurrently reset. Skip storing by scheduler",
                    key.getStreamName(), key.getDirection());
            return;
        }
        try {
            storeBatchAsync(batch);
        } catch (Exception ex) {
            if (logger.isErrorEnabled()) {
                logger.error("Cannot store batch for session {}: {}", key, formatStoredMessageBatch(batch, false), ex);
            }
        }
    }

    protected abstract String[] getAttributes();

    protected abstract List<M> getMessages(T delivery);

    protected abstract MessageToStore convert(M originalMessage);

    protected abstract CompletableFuture<Void> store(StoredMessageBatch storedMessageBatch);

    protected abstract SequenceToTimestamp extractSequenceToTimestamp(M message);

    protected abstract SessionKey createSessionKey(M message);

    protected abstract String shortDebugString(T batch);

    protected static class SessionKey {
        private final String streamName;
        private final Direction direction;

        public SessionKey(String streamName, Direction direction) {
            this.streamName = requireNonNull(streamName, "'Stream name' parameter");
            this.direction = requireNonNull(direction, "'Direction' parameter");
        }

        public String getStreamName() {
            return streamName;
        }

        public Direction getDirection() {
            return direction;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            SessionKey that = (SessionKey)obj;

            return new EqualsBuilder()
                    .append(streamName, that.streamName)
                    .append(direction, that.direction)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder()
                    .append(streamName)
                    .append(direction)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, NO_CLASS_NAME_STYLE)
                    .append("streamName", streamName)
                    .append("direction", direction)
                    .toString();
        }
    }

    private static class SessionData {
        private final AtomicReference<SequenceToTimestamp> lastSequenceToTimestamp = new AtomicReference<>(SequenceToTimestamp.MIN);

        private final SessionBatchHolder batchHolder;

        SessionData(Supplier<StoredMessageBatch> batchSupplier) {
            batchHolder = new SessionBatchHolder(requireNonNull(batchSupplier, "'batchSupplier' cannot be null"));
        }

        public SequenceToTimestamp getAndUpdateLastSequenceToTimestamp(SequenceToTimestamp newLastSequenceToTimestamp) {
            return lastSequenceToTimestamp.getAndAccumulate(newLastSequenceToTimestamp, maxBy(SEQUENCE_TO_TIMESTAMP_COMPARATOR));
        }

        public SessionBatchHolder getBatchHolder() {
            return batchHolder;
        }
    }
}
