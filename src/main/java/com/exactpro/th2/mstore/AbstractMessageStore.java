/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
import static org.apache.commons.lang3.builder.ToStringStyle.NO_CLASS_NAME_STYLE;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.jetbrains.annotations.NotNull;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.mstore.cfg.MessageStoreConfiguration;
import com.exactpro.th2.store.common.AbstractStorage;
import com.google.protobuf.GeneratedMessageV3;

public abstract class AbstractMessageStore<T extends GeneratedMessageV3, M extends GeneratedMessageV3> extends AbstractStorage<T> {
    private final ScheduledExecutorService drainExecutor = Executors.newSingleThreadScheduledExecutor();
    private final Map<SessionKey, SessionBatchHolder> sessionToHolder = new ConcurrentHashMap<>();
    private final MessageStoreConfiguration configuration;
    private final Map<CompletableFuture<Void>, StoredMessageBatch> asyncStoreFutures = new ConcurrentHashMap<>();
    private volatile ScheduledFuture<?> future;

    public AbstractMessageStore(
            MessageRouter<T> router,
            @NotNull CradleManager cradleManager,
            @NotNull MessageStoreConfiguration configuration
    ) {
        super(router, cradleManager);
        this.configuration = Objects.requireNonNull(configuration, "'Configuration' parameter");
    }

    @Override
    public void start() {
        super.start();
        future = drainExecutor.scheduleAtFixedRate(this::drainByScheduler, configuration.getDrainInterval(), configuration.getDrainInterval(), TimeUnit.MILLISECONDS);
        logger.info("Drain scheduler is started");
    }

    @Override
    public void dispose() {
        super.dispose();
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
                        logger.warn("{} - storing {} batch is failure", getClass().getSimpleName(), formatStoredMessageBatch(batch, false), e);
                    }
                    futuresToRemove.add(future);
                } catch (TimeoutException | InterruptedException e) {
                    if (logger.isErrorEnabled()) {
                        logger.error("{} - future related to {} batch can't be complited", getClass().getSimpleName(), formatStoredMessageBatch(batch, false), e);
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

    @Override
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
            SessionBatchHolder holder = sessionToHolder.computeIfAbsent(sessionKey, ignore ->
                    new SessionBatchHolder(cradleStorage.getObjectsFactory()::createMessageBatch));
            long prevLastSeq = holder.getAndUpdateSequence(lastSequence);
            if (prevLastSeq >= firstSequence) {
                logger.error("Duplicated batch found: {}", shortDebugString(messageBatch));
                return;
            }
            storeMessages(messages, holder);
        } catch (Exception ex) {
            logger.error("Cannot handle the batch of type {}", messageBatch.getClass(), ex);
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
        asyncStoreFutures.put(future, holtBatch);
        future.whenCompleteAsync((value, exception) -> {
            try {
                if (exception == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} - batch stored: {}", getClass().getSimpleName(), formatStoredMessageBatch(holtBatch, true));
                    }
                } else {
                    if (logger.isErrorEnabled()) {
                        logger.error("{} - batch storing is failure: {}", getClass().getSimpleName(), formatStoredMessageBatch(holtBatch, true), exception);
                    }
                }
            } finally {
                if (asyncStoreFutures.remove(future) == null) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("{} - future related to batch {} already removed", getClass().getSimpleName(), formatStoredMessageBatch(holtBatch, true));
                    }
                }
            }
        });
    }

    private static String formatStoredMessageBatch(StoredMessageBatch storedMessageBatch, boolean full) {
        ToStringBuilder builder = new ToStringBuilder(storedMessageBatch, NO_CLASS_NAME_STYLE)
                .append("stream", storedMessageBatch.getStreamName())
                .append("direction", storedMessageBatch.getId().getDirection())
                .append("batch id", storedMessageBatch.getId().getIndex());
        if (full) {
            builder.append("size", storedMessageBatch.getBatchSize())
                    .append("count", storedMessageBatch.getMessageCount())
                    .append("message sequences", storedMessageBatch.getMessages().stream()
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
        sessionToHolder.forEach((key, holder) -> drainHolder(key, holder, force));
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
            logger.error("Cannot store batch for session {}", key, ex);
        }
    }

    protected abstract List<M> getMessages(T delivery);

    protected abstract MessageToStore convert(M originalMessage);

    protected abstract CompletableFuture<Void> store(StoredMessageBatch storedMessageBatch);

    protected abstract long extractSequence(M message);

    protected abstract SessionKey createSessionKey(M message);

    protected static class SessionKey {
        private final String streamName;
        private final Direction direction;

        public SessionKey(String streamName, Direction direction) {
            this.streamName = Objects.requireNonNull(streamName, "'Stream name' parameter");
            this.direction = Objects.requireNonNull(direction, "'Direction' parameter");
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
}
