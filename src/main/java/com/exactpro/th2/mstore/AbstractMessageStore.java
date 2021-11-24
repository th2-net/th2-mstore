/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import static com.google.protobuf.TextFormat.shortDebugString;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.builder.ToStringStyle.NO_CLASS_NAME_STYLE;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.MessageBatchToStore;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.mstore.cfg.MessageStoreConfiguration;
import com.google.protobuf.GeneratedMessageV3;

public abstract class AbstractMessageStore<T extends GeneratedMessageV3, M extends GeneratedMessageV3> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMessageStore.class);

    protected final CradleStorage cradleStorage;
    private final ScheduledExecutorService drainExecutor = Executors.newSingleThreadScheduledExecutor();
    private final Map<SessionKey, SessionData> sessionToHolder = new ConcurrentHashMap<>();
    private final MessageStoreConfiguration configuration;
    private final Map<CompletableFuture<Void>, MessageBatchToStore> asyncStoreFutures = new ConcurrentHashMap<>();
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
                        logger.warn("{} - storing {} batch is failure", getClass().getSimpleName(), formatMessageBatchToStore(batch, false), e);
                    }
                    futuresToRemove.add(future);
                } catch (TimeoutException | InterruptedException e) {
                    if (logger.isErrorEnabled()) {
                        logger.error("{} - future related to {} batch can't be completed", getClass().getSimpleName(), formatMessageBatchToStore(batch, false), e);
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
            SessionKey sessionKey = createSessionKey(lastMessage);
            SessionData sessionData = sessionToHolder.computeIfAbsent(sessionKey, ignore ->
                    new SessionData(cradleStorage.getEntitiesFactory()::messageBatch));
            long prevLastSeq = sessionData.getAndUpdateSequence(lastSequence);
            if (prevLastSeq >= firstSequence) {
                logger.error("Duplicated batch found: {}", shortDebugString(messageBatch));
                return;
            }
            storeMessages(messages, sessionData.getBatchHolder());
        } catch (Exception ex) {
            logger.error("Cannot handle the batch of type {}", messageBatch.getClass(), ex);
        }
    }

    protected void storeMessages(List<M> messagesList, SessionBatchHolder holder) throws CradleStorageException, IOException {
        logger.debug("Process {} messages started", messagesList.size());

        MessageBatchToStore messageBatchToStore = cradleStorage.getEntitiesFactory().messageBatch();
        for (M message : messagesList) {
            MessageToStore messageToStore = convert(message);
            messageBatchToStore.addMessage(messageToStore);
        }
        MessageBatchToStore holtBatch;
        synchronized (holder) {
            if (holder.add(messageBatchToStore)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Message Batch added to the holder: {}", formatMessageBatchToStore(messageBatchToStore, true));
                }
                return;
            }
            holtBatch = holder.resetAndUpdate(messageBatchToStore);
        }

        if (holtBatch.isEmpty()) {
            logger.debug("Holder for session alias: '{}', direction: '{}' has been concurrently reset. Skip storing",
                    messageBatchToStore.getSessionAlias(), messageBatchToStore.getDirection());
        } else {
            storeBatchAsync(holtBatch);
        }
    }

    private void storeBatchAsync(MessageBatchToStore holtBatch) throws CradleStorageException, IOException {
        CompletableFuture<Void> future = store(holtBatch);
        asyncStoreFutures.put(future, holtBatch);
        future.whenCompleteAsync((value, exception) -> {
            try {
                if (exception == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} - batch stored: {}", getClass().getSimpleName(), formatMessageBatchToStore(holtBatch, true));
                    }
                } else {
                    if (logger.isErrorEnabled()) {
                        logger.error("{} - batch storing is failure: {}", getClass().getSimpleName(), formatMessageBatchToStore(holtBatch, true), exception);
                    }
                }
            } finally {
                if (asyncStoreFutures.remove(future) == null) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("{} - future related to batch {} already removed", getClass().getSimpleName(), formatMessageBatchToStore(holtBatch, true));
                    }
                }
            }
        });
    }

    private static String formatMessageBatchToStore(MessageBatchToStore messageBatchToStore, boolean full) {
        ToStringBuilder builder = new ToStringBuilder(messageBatchToStore, NO_CLASS_NAME_STYLE)
                .append("session alias", messageBatchToStore.getSessionAlias())
                .append("direction", messageBatchToStore.getId().getDirection())
                .append("batch id", messageBatchToStore.getId().getSequence());
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
        sessionToHolder.forEach((key, sessionData) -> drainHolder(key, sessionData.getBatchHolder(), force));
    }

    private void drainHolder(SessionKey key, SessionBatchHolder holder, boolean force) {
        logger.trace("Drain holder for session {}; force: {}", key, force);
        MessageBatchToStore batch;
        synchronized (holder) {
            if (!force && !holder.isReadyToReset(configuration.getDrainInterval())) {
                return;
            }
            batch = holder.reset();
        }
        if (batch.isEmpty()) {
            logger.debug("Holder for stream: '{}', direction: '{}' has been concurrently reset. Skip storing by scheduler",
                    key.getSessionAlias(), key.getDirection());
            return;
        }
        try {
            storeBatchAsync(batch);
        } catch (Exception ex) {
            logger.error("Cannot store batch for session {}", key, ex);
        }
    }

    protected abstract String[] getAttributes();

    protected abstract List<M> getMessages(T delivery);

    protected abstract MessageToStore convert(M originalMessage) throws CradleStorageException;

    protected abstract CompletableFuture<Void> store(MessageBatchToStore messageBatchToStore) throws CradleStorageException, IOException;

    protected abstract long extractSequence(M message);

    protected abstract SessionKey createSessionKey(M message);

    protected static class SessionKey {
        private final String sessionAlias;
        private final Direction direction;

        public SessionKey(String sessionAlias, Direction direction) {
            this.sessionAlias = Objects.requireNonNull(sessionAlias, "'Stream name' parameter");
            this.direction = Objects.requireNonNull(direction, "'Direction' parameter");
        }

        public String getSessionAlias() {
            return sessionAlias;
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
                    .append(sessionAlias, that.sessionAlias)
                    .append(direction, that.direction)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder()
                    .append(sessionAlias)
                    .append(direction)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, NO_CLASS_NAME_STYLE)
                    .append("sessionAlias", sessionAlias)
                    .append("direction", direction)
                    .toString();
        }
    }

    private static class SessionData {
        private final AtomicLong lastSequence = new AtomicLong(Long.MIN_VALUE);

        private final SessionBatchHolder batchHolder;

        SessionData(Supplier<MessageBatchToStore> batchSupplier) {
            batchHolder = new SessionBatchHolder(Objects.requireNonNull(batchSupplier, "'batchSupplier' cannot be null"));
        }

        public long getAndUpdateSequence(long newLastSeq) {
            return lastSequence.getAndAccumulate(newLastSeq, Math::max);
        }

        public SessionBatchHolder getBatchHolder() {
            return batchHolder;
        }
    }
}
