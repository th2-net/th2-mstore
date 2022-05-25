/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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
import com.exactpro.cradle.messages.MessageBatchToStore;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.mstore.cfg.MessageStoreConfiguration;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.builder.ToStringStyle.NO_CLASS_NAME_STYLE;

public abstract class AbstractMessageStore<T extends GeneratedMessageV3, M extends GeneratedMessageV3> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMessageStore.class);

    protected final CradleStorage cradleStorage;
    private final ScheduledExecutorService drainExecutor = Executors.newSingleThreadScheduledExecutor();
    private final Map<SessionKey, SessionData> sessionToHolder = new ConcurrentHashMap<>();
    private final MessageStoreConfiguration configuration;
    private final Map<CompletableFuture<Void>, BatchData> asyncStoreFutures = new ConcurrentHashMap<>();
    private volatile ScheduledFuture<?> future;
    private final MessageRouter<T> router;
    private SubscriberMonitor monitor;

    public AbstractMessageStore(
            @NotNull MessageRouter<T> router,
            @NotNull CradleManager cradleManager,
            @NotNull MessageStoreConfiguration configuration
    ) {
        this.router = requireNonNull(router, "Message router can't be null");
        cradleStorage = requireNonNull(cradleManager.getStorage(), "Cradle storage can't be null");
        this.configuration = Objects.requireNonNull(configuration, "'Configuration' parameter");
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
        future = drainExecutor.scheduleAtFixedRate(this::drainByScheduler, configuration.getDrainInterval(), configuration.getDrainInterval(), TimeUnit.MILLISECONDS);
        logger.info("Drain scheduler is started");
    }

    public void dispose() {
        if (monitor != null) {
            try {
                monitor.unsubscribe();
            } catch (Exception e) {
                logger.error("Can not unsubscribe from queues", e);
            }
        }
        try {
            ScheduledFuture<?> future = this.future;
            if (future != null) {
                this.future = null;
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

        awaitFutures();
    }

    private void awaitFutures() {
        logger.debug("Waiting for futures completion");
        Collection<CompletableFuture<Void>> futuresToRemove = new HashSet<>();
        while (!(asyncStoreFutures.isEmpty() || Thread.currentThread().isInterrupted())) {
            logger.info("Wait for the completion of {} futures", asyncStoreFutures.size());
            futuresToRemove.clear();
            asyncStoreFutures.forEach((future, batch) -> {
                try {
                    if (!future.isDone()) {
                        future.get(1, TimeUnit.SECONDS);
                    }
                    futuresToRemove.add(future);
                } catch (CancellationException | ExecutionException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("{} - failed storing {} batch", getClass().getSimpleName(),
                                formatMessageBatchToStore(batch, false), e);
                    }
                    futuresToRemove.add(future);
                } catch (TimeoutException | InterruptedException e) {
                    if (logger.isErrorEnabled()) {
                        logger.error("{} - future related to {} batch can't be completed", getClass().getSimpleName(),
                                formatMessageBatchToStore(batch, false), e);
                    }
                    boolean mayInterruptIfRunning = e instanceof InterruptedException;
                    future.cancel(mayInterruptIfRunning);

                    if (mayInterruptIfRunning) {
                        Thread.currentThread().interrupt();
                    }
                }
            });
            asyncStoreFutures.keySet().removeAll(futuresToRemove);
        }
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
            M lastMessage = messages.get(messages.size() - 1);
            long firstSequence = extractSequence(messages.get(0));
            long lastSequence = extractSequence(lastMessage);
            SessionData sessionData = sessionToHolder.computeIfAbsent(
                    createSessionKey(lastMessage),
                    sessionKey -> new SessionData(sessionKey, cradleStorage.getEntitiesFactory()::messageBatch));
            long prevLastSeq = sessionData.getAndUpdateSequence(lastSequence);
            if (prevLastSeq >= firstSequence) {
                logger.error("Duplicated batch found: {}", shortDebugString(messageBatch));
                return;
            }
            storeMessages(messages, sessionData);
        } catch (Exception ex) {
            logger.error("Cannot handle the batch of type {}", messageBatch.getClass(), ex);
        }
    }

    protected void storeMessages(List<M> messagesList, SessionData sessionData) throws CradleStorageException, IOException {
        logger.debug("Process {} messages started", messagesList.size());

        MessageBatchToStore messageBatchToStore = cradleStorage.getEntitiesFactory().messageBatch();
        for (M message : messagesList) {
            MessageToStore messageToStore = convert(message);
            messageBatchToStore.addMessage(messageToStore);
        }
        MessageBatchToStore holtBatch;
        String sessionGroup = sessionData.sessionKey.sessionGroup;
        SessionBatchHolder holder = sessionData.batchHolder;
        synchronized (holder) {
            if (holder.add(messageBatchToStore)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Message Batch added to the holder: {}",
                            formatMessageBatchToStore(new BatchData(messageBatchToStore, sessionGroup), true));
                }
                return;
            }
            holtBatch = holder.resetAndUpdate(messageBatchToStore);
        }

        if (holtBatch.isEmpty()) {
            logger.debug("Holder for '{}' has been concurrently reset. Skip storing", sessionData.sessionKey);
        } else {
            storeBatchAsync(new BatchData(holtBatch, sessionGroup));
        }
    }


    private void storeBatchAsync(BatchData batchData) throws CradleStorageException, IOException {
        CompletableFuture<Void> future = store(batchData);
        asyncStoreFutures.put(future, batchData);
        future.whenCompleteAsync((value, exception) -> {
            try {
                if (exception == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} - batch stored: {}", getClass().getSimpleName(),
                                formatMessageBatchToStore(batchData, true));
                    }
                } else {
                    if (logger.isErrorEnabled()) {
                        logger.error("{} - failed storing batch: {}", getClass().getSimpleName(),
                                formatMessageBatchToStore(batchData, true), exception);
                    }
                }
            } finally {
                if (asyncStoreFutures.remove(future) == null) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("{} - future related to batch {} already removed", getClass().getSimpleName(),
                                formatMessageBatchToStore(batchData, true));
                    }
                }
            }
        });
    }

    private static String formatMessageBatchToStore(BatchData batchData, boolean full) {
        MessageBatchToStore messageBatchToStore = batchData.batch;
        StoredMessageId id = messageBatchToStore.getId();
        ToStringBuilder builder = new ToStringBuilder(messageBatchToStore, NO_CLASS_NAME_STYLE)
                .append("book name", id.getBookId().getName())
                .append("session alias", id.getSessionAlias())
                .append("session group", batchData.sessionGroup)
                .append("direction", id.getDirection())
                .append("batch id", id.getSequence());
        if (full) {
            builder.append("size", messageBatchToStore.getBatchSize())
                    .append("count", messageBatchToStore.getMessageCount())
                    .append("message sequences", messageBatchToStore.getMessages().stream()
                            .map(StoredMessage::getId)
                            .map(StoredMessageId::getSequence)
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
        SessionKey lastKey = null;
        long lastSeq = Long.MIN_VALUE;
        for (int i = 0; i < messages.size(); i++) {
            M message = messages.get(i);
            SessionKey sessionKey = createSessionKey(message);
            if (lastKey == null) {
                lastKey = sessionKey;
            } else {
                verifySession(i, lastKey, sessionKey);
            }

            long currentSeq = extractSequence(message);
            verifySequence(i, lastSeq, currentSeq);
            lastSeq = currentSeq;
        }

    }

    private static void verifySequence(int messageIndex, long lastSeq, long currentSeq) {
        if (lastSeq >= currentSeq) {
            throw new IllegalArgumentException(
                    String.format(
                            "Delivery contains unordered messages. Message [%d] - seqN %d; Message [%d] - seqN %d",
                            messageIndex - 1, lastSeq, messageIndex, currentSeq
                    )
            );
        }
    }

    private static void verifySession(int messageIndex, SessionKey lastKey, SessionKey sessionKey) {
        if (!lastKey.equals(sessionKey)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Delivery contains different sessions. Message [%d] - session %s; Message [%d] - session %s",
                            messageIndex - 1, lastKey, messageIndex, sessionKey
                    )
            );
        }
    }

    private void drainByScheduler() {
        logger.debug("Start storing batches by scheduler");
        drain(false);
        logger.debug("Stop storing batches by scheduler");
    }

    private void drain(boolean force) {
        sessionToHolder.forEach((key, sessionData) -> drainHolder(sessionData, force));
    }

    private void drainHolder(SessionData sessionData, boolean force) {
        SessionKey sessionKey = sessionData.sessionKey;
        SessionBatchHolder holder = sessionData.batchHolder;
        logger.trace("Drain holder for session {}; force: {}", sessionKey, force);
        MessageBatchToStore batch;
        synchronized (holder) {
            if (!force && !holder.isReadyToReset(configuration.getDrainInterval())) {
                return;
            }
            batch = holder.reset();
        }
        if (batch.isEmpty()) {
            logger.debug("Holder for stream: '{}' has been concurrently reset. Skip storing by scheduler", sessionKey);
            return;
        }
        BatchData batchData = new BatchData(batch, sessionKey.sessionGroup);
        try {
            storeBatchAsync(batchData);
        } catch (Exception ex) {
            if (logger.isErrorEnabled()) {
                logger.error("Cannot store batch for session {}: {}", sessionKey,
                        formatMessageBatchToStore(batchData, false), ex);
            }
        }
    }

    protected abstract String[] getAttributes();

    protected abstract List<M> getMessages(T delivery);

    protected abstract MessageToStore convert(M originalMessage) throws CradleStorageException;

    protected abstract CompletableFuture<Void> store(BatchData batchData) throws CradleStorageException, IOException;

    protected abstract long extractSequence(M message);

    protected abstract SessionKey createSessionKey(M message);

    protected static class SessionKey {
        public final String sessionAlias;
        public final String sessionGroup;
        public final Direction direction;
        public final String bookName;

        public SessionKey(MessageID messageID) {
            this.sessionAlias = Objects.requireNonNull(messageID.getConnectionId().getSessionAlias(), "'Session alias' parameter");
            this.sessionGroup = requireNonNull(messageID.getConnectionId().getSessionGroup(), "'Session group' parameter");
            this.direction = Objects.requireNonNull(toCradleDirection(messageID.getDirection()), "'Direction' parameter");
            this.bookName = Objects.requireNonNull(messageID.getBookName(), "'Book name' parameter");
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
                    .append(sessionAlias, that.sessionAlias)
                    .append(sessionGroup, that.sessionGroup)
                    .append(direction, that.direction)
                    .append(bookName, that.bookName)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder()
                    .append(sessionAlias)
                    .append(sessionGroup)
                    .append(direction)
                    .append(bookName)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, NO_CLASS_NAME_STYLE)
                    .append("sessionAlias", sessionAlias)
                    .append("sessionGroup", sessionGroup)
                    .append("direction", direction)
                    .append("bookName", bookName)
                    .toString();
        }
    }

    public static class BatchData {
        public final MessageBatchToStore batch;
        public final String sessionGroup;
        public BatchData(MessageBatchToStore holtBatch, String sessionGroup) {
            this.batch = holtBatch;
            this.sessionGroup = sessionGroup;
        }
    }

    static class SessionData {
        public final SessionBatchHolder batchHolder;
        public final SessionKey sessionKey;
        private final AtomicLong lastSequence = new AtomicLong(Long.MIN_VALUE);

        SessionData(SessionKey sessionKey, Supplier<MessageBatchToStore> batchSupplier) {
            this.sessionKey = sessionKey;
            this.batchHolder = new SessionBatchHolder(requireNonNull(batchSupplier, "'batchSupplier' cannot be null"));
        }

        public long getAndUpdateSequence(long newLastSeq) {
            return lastSequence.getAndAccumulate(newLastSeq, Math::max);
        }
    }
}
