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
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
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
import java.util.stream.Collectors;

import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.builder.ToStringStyle.NO_CLASS_NAME_STYLE;

public abstract class AbstractMessageStore<T extends GeneratedMessageV3, M extends GeneratedMessageV3> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMessageStore.class);

    protected final CradleStorage cradleStorage;
    private final ScheduledExecutorService drainExecutor = Executors.newSingleThreadScheduledExecutor();
    private final Map<SessionKey, SessionData> sessionData = new ConcurrentHashMap<>();
    private final Map<String, BatchHolder> batchHolder = new ConcurrentHashMap<>();
    private final MessageStoreConfiguration configuration;
    private final Map<CompletableFuture<Void>, GroupedMessageBatchToStore> asyncStoreFutures = new ConcurrentHashMap<>();
    private volatile ScheduledFuture<?> future;
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

            String sessionGroup = null;
            for (M message: messages) {
                long sequence = extractSequence(message);
                SessionKey sessionKey = createSessionKey(message);

                if (sessionGroup != null && !sessionGroup.equals(sessionKey.sessionGroup)) {
                    logger.error("Batch contains mixed group messages", shortDebugString(messageBatch));
                    return;
                }
                sessionGroup = sessionKey.sessionGroup;
                SessionData sessionData = this.sessionData.computeIfAbsent(sessionKey, k -> new SessionData());

                long prevSequence = sessionData.getAndUpdateSequence(sequence);
                if (prevSequence >= sequence) {
                    logger.error("Duplicated batch found: {}", shortDebugString(messageBatch));
                    return;
                }

            }

            storeMessages(messages, sessionGroup);
        } catch (Exception ex) {
            logger.error("Cannot handle the batch of type {}", messageBatch.getClass(), ex);
        }
    }


    protected void storeMessages(List<M> messagesList, String sessionGroup) throws CradleStorageException, IOException {
        logger.debug("Process {} messages started", messagesList.size());

        GroupedMessageBatchToStore batch = cradleStorage.getEntitiesFactory().groupedMessageBatch(sessionGroup);
        for (M message : messagesList) {
            MessageToStore messageToStore = convert(message);
            batch.addMessage(messageToStore);
        }
        GroupedMessageBatchToStore holtBatch;
        BatchHolder holder = batchHolder.computeIfAbsent(sessionGroup,
                k -> new BatchHolder(sessionGroup, () -> cradleStorage.getEntitiesFactory().groupedMessageBatch(sessionGroup)));

        synchronized (holder) {
            if (holder.add(batch)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Message Batch added to the holder: {}", formatMessageBatchToStore(batch, true));
                }
                return;
            }
            holtBatch = holder.resetAndUpdate(batch);
        }

        if (holtBatch.isEmpty()) {
            logger.debug("Holder for '{}' has been concurrently reset. Skip storing", sessionGroup);
        } else {
            storeBatchAsync(holtBatch);
        }
    }


    private void storeBatchAsync(GroupedMessageBatchToStore batch) throws CradleStorageException, IOException {
        CompletableFuture<Void> future = store(batch);
        asyncStoreFutures.put(future, batch);
        future.whenCompleteAsync((value, exception) -> {
            try {
                if (exception == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} - batch stored: {}", getClass().getSimpleName(),
                                formatMessageBatchToStore(batch, true));
                    }
                } else {
                    if (logger.isErrorEnabled()) {
                        logger.error("{} - failed storing batch: {}", getClass().getSimpleName(),
                                formatMessageBatchToStore(batch, true), exception);
                    }
                }
            } finally {
                if (asyncStoreFutures.remove(future) == null) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("{} - future related to batch {} already removed", getClass().getSimpleName(),
                                formatMessageBatchToStore(batch, true));
                    }
                }
            }
        });
    }


    private static String formatMessageBatchToStore(GroupedMessageBatchToStore batch, boolean full) {
        ToStringBuilder builder = new ToStringBuilder(batch, NO_CLASS_NAME_STYLE)
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
        batchHolder.forEach((key, batchHodler) -> drainHolder(batchHodler, force));
    }

    private void drainHolder(BatchHolder holder, boolean force) {

        logger.trace("Drain holder for session {}; force: {}", holder.getGroup(), force);
        GroupedMessageBatchToStore batch;
        synchronized (holder) {
            if (!force && !holder.isReadyToReset(configuration.getDrainInterval())) {
                return;
            }
            batch = holder.reset();
        }
        if (batch.isEmpty()) {
            logger.debug("Holder for group: '{}' has been concurrently reset. Skip storing by scheduler", holder.getGroup());
            return;
        }

        try {
            storeBatchAsync(batch);
        } catch (Exception ex) {
            if (logger.isErrorEnabled()) {
                logger.error("Cannot store batch for group {}: {}", holder.getGroup(),
                        formatMessageBatchToStore(batch, false), ex);
            }
        }
    }

    protected abstract String[] getAttributes();

    protected abstract List<M> getMessages(T delivery);

    protected abstract MessageToStore convert(M originalMessage) throws CradleStorageException;

    protected abstract CompletableFuture<Void> store(GroupedMessageBatchToStore batch) throws CradleStorageException, IOException;

    protected abstract long extractSequence(M message);

    protected abstract SessionKey createSessionKey(M message);

    protected static class SessionKey {
        public final String sessionAlias;
        public final String sessionGroup;
        public final Direction direction;
        public final String bookName;

        public SessionKey(MessageID messageID) {
            this.sessionAlias = Objects.requireNonNull(messageID.getConnectionId().getSessionAlias(), "'Session alias' parameter");
            this.direction = Objects.requireNonNull(toCradleDirection(messageID.getDirection()), "'Direction' parameter");
            this.bookName = Objects.requireNonNull(messageID.getBookName(), "'Book name' parameter");

            String group = messageID.getConnectionId().getSessionGroup();
            this.sessionGroup = (group == null || group.isEmpty()) ? this.sessionAlias : group;
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

    static class SessionData {
        //public final SessionBatchHolder batchHolder;
        private final AtomicLong lastSequence = new AtomicLong(Long.MIN_VALUE);

        SessionData() {
        }

        public long getAndUpdateSequence(long newLastSeq) {
            return lastSequence.getAndAccumulate(newLastSeq, Math::max);
        }
    }
}
