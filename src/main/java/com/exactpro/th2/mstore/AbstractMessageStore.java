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
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredGroupMessageBatch;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.utils.CradleStorageException;
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
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.exactpro.th2.common.message.MessageUtils.toTimestamp;
import static com.exactpro.th2.common.util.StorageUtils.toInstant;
import static com.exactpro.th2.mstore.SequenceToTimestamp.SEQUENCE_TO_TIMESTAMP_COMPARATOR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.BinaryOperator.maxBy;
import static org.apache.commons.lang3.builder.ToStringStyle.NO_CLASS_NAME_STYLE;

public abstract class AbstractMessageStore<T extends GeneratedMessageV3, M extends GeneratedMessageV3> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMessageStore.class);

    protected final CradleStorage cradleStorage;
    private final ScheduledExecutorService drainExecutor = Executors.newSingleThreadScheduledExecutor();
    private final Map<SessionKey, SessionData> sessionData = new ConcurrentHashMap<>();
    private final Map<String, SessionBatchHolder> sessionHolder = new ConcurrentHashMap<>();
    private final MessageStoreConfiguration configuration;
    private final Map<CompletableFuture<Void>, StoredGroupMessageBatch> asyncStoreFutures = new ConcurrentHashMap<>();
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

    public void handle(T messageBatch) {
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
            for (M message : messages) {
                SequenceToTimestamp sequenceToTimestamp = extractSequenceToTimestamp(message);
                SessionKey sessionKey = createSessionKey(message);

                sessionGroup = sessionKey.getSessionGroup();
                SessionData sessionData = this.sessionData.computeIfAbsent(sessionKey, k -> new SessionData());

                sessionData.getAndUpdateLastSequenceToTimestamp(sequenceToTimestamp).getSequence();
            }

            storeMessages(messages, sessionGroup);
        } catch (Exception ex) {
            if (logger.isErrorEnabled()) {
                logger.error("Cannot handle the batch of type {} message id {}", messageBatch.getClass(), shortDebugString(messageBatch), ex);
            }
        }
    }

    protected void storeMessages(List<M> messagesList, String sessionGroup) throws CradleStorageException {
        logger.debug("Process {} messages started", messagesList.size());

        StoredGroupMessageBatch storedGroupMessageBatch = cradleStorage.getObjectsFactory().createGroupMessageBatch();
        for (M message : messagesList) {
            MessageToStore messageToStore = convert(message);
            storedGroupMessageBatch.addMessage(messageToStore);
        }
        StoredGroupMessageBatch holtBatch;
        SessionBatchHolder holder = sessionHolder.computeIfAbsent(sessionGroup,
                k -> new SessionBatchHolder(sessionGroup, () -> cradleStorage.getObjectsFactory().createGroupMessageBatch()));
        synchronized (holder) {
            if (holder.add(storedGroupMessageBatch)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Message Batch added to the holder: {}", formatStoredMessageBatch(storedGroupMessageBatch, true));
                }
                return;
            }
            holtBatch = holder.resetAndUpdate(storedGroupMessageBatch);
        }

        if (holtBatch.isEmpty()) {
            logger.debug("Holder for '{}' has been concurrently reset. Skip storing", storedGroupMessageBatch.getSessionGroup());
        } else {
            storeBatchAsync(holtBatch, sessionGroup);
        }
    }

    private void storeBatchAsync(StoredGroupMessageBatch holtBatch, String sessionGroup) {
        CompletableFuture<Void> future = store(holtBatch, sessionGroup);
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

    /**
     * Checks that the delivery contains all messages related to one session
     * and that each message has sequence number greater than the previous one.
     * @param delivery the delivery received from router
     */
    private void verifyBatch(T delivery) {
        List<M> messages = getMessages(delivery);
        Map<SessionKey, M> sessions = new HashMap<>();
        SessionKey previousKey = null;
        for (int i = 0; i < messages.size(); i++) {
            M message = messages.get(i);
            SessionKey sessionKey = createSessionKey(message);
            if (previousKey == null) {
                previousKey = sessionKey;
            }
            if(!previousKey.getSessionGroup().equals(sessionKey.getSessionGroup())){
                throw new IllegalArgumentException(format(
                        "Delivery contains different session groups. Message [%d] - sequence %d; Message [%d] - sequence %d",
                        i - 1,
                        previousKey,
                        i,
                        sessionKey
                ));
            }
            SequenceToTimestamp currentSequenceToTimestamp = extractSequenceToTimestamp(message);
            SequenceToTimestamp previousSequenceToTimestamp;
            if (sessions.containsKey(sessionKey)) {
                previousSequenceToTimestamp = extractSequenceToTimestamp(sessions.get(sessionKey));
            } else if(sessionData.containsKey(sessionKey)){
                previousSequenceToTimestamp = sessionData.get(sessionKey).lastSequenceToTimestamp.get();
            } else {
                previousSequenceToTimestamp = getLastSequenceToTimeStamp(sessionKey);
            }

            verifySequenceToTimestamp(i, previousSequenceToTimestamp, currentSequenceToTimestamp);
            sessions.put(sessionKey, message);
        }
    }
    private SequenceToTimestamp getLastSequenceToTimeStamp(SessionKey sessionKey){
        long lastSequence = -1L;
        try {
            lastSequence = cradleStorage.getLastMessageIndex(sessionKey.session, sessionKey.direction);
        } catch (IOException e) {
            logger.info("Couldn't get sequence of last message from cradle: {}", e.getMessage());
        }
        Instant lastTimeInstant = Instant.MIN;
        StoredMessageId storedMsgId = new StoredMessageId(sessionKey.session, sessionKey.direction, lastSequence);
        try {
            StoredMessage message = cradleStorage.getMessage(storedMsgId);
            if(message != null){
                lastTimeInstant = message.getTimestamp();
            }
        } catch (IOException e) {
            logger.info("Couldn't get timestamp of last message from cradle: {}", e.getMessage());
        }
        return new SequenceToTimestamp(lastSequence, toTimestamp(lastTimeInstant));
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
        try {
            storeBatchAsync(batch, group);
        } catch (Exception ex) {
            if (logger.isErrorEnabled()) {
                logger.error("Cannot store batch for group {}: {}", group, formatStoredMessageBatch(batch, false), ex);
            }
        }
    }

    protected abstract String[] getAttributes();

    protected abstract List<M> getMessages(T delivery);

    protected abstract MessageToStore convert(M originalMessage);

    protected abstract CompletableFuture<Void> store(StoredGroupMessageBatch messageBatch, String sessionGroup);

    protected abstract SequenceToTimestamp extractSequenceToTimestamp(M message);

    protected abstract SessionKey createSessionKey(M message);

    protected abstract String shortDebugString(T batch);

    protected static class SessionKey {
        private final String session;
        private final String sessionGroup;
        private final Direction direction;

        public SessionKey(String session, Direction direction, String sessionGroup) {
            this.session = requireNonNull(session, "'session' parameter");
            this.direction = requireNonNull(direction, "'direction' parameter");
            this.sessionGroup = requireNonNull(sessionGroup, "'group' parameter");
        }

        public String getSession() {
            return session;
        }

        public String getSessionGroup() {
            return sessionGroup;
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
                    .append(getSession(), that.getSession())
                    .append(getDirection(), that.getDirection())
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder()
                    .append(getSession())
                    .append(getDirection())
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, NO_CLASS_NAME_STYLE)
                    .append("session", getSession())
                    .append("direction", getDirection())
                    .toString();
        }
    }

    static class SessionData {
        private final AtomicReference<SequenceToTimestamp> lastSequenceToTimestamp = new AtomicReference<>(SequenceToTimestamp.MIN);

        SessionData() {
        }

        public SequenceToTimestamp getAndUpdateLastSequenceToTimestamp(SequenceToTimestamp newLastSequenceToTimestamp) {
            return lastSequenceToTimestamp.getAndAccumulate(newLastSequenceToTimestamp, maxBy(SEQUENCE_TO_TIMESTAMP_COMPARATOR));
        }
    }
}
