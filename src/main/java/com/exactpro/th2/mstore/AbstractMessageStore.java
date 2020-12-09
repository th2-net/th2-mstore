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

import static com.exactpro.cradle.messages.StoredMessageBatch.MAX_MESSAGES_COUNT;
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
import java.util.concurrent.atomic.AtomicLong;
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
import com.exactpro.th2.store.common.AbstractStorage;
import com.google.protobuf.GeneratedMessageV3;

public abstract class AbstractMessageStore<T extends GeneratedMessageV3, M extends GeneratedMessageV3> extends AbstractStorage<T> {
    private final Map<SessionKey, AtomicLong> sessionToLastSequence = new ConcurrentHashMap<>();
    private final Map<CompletableFuture<Void>, StoredMessageBatch> asyncStoreFutures = new ConcurrentHashMap<>();

    public AbstractMessageStore(MessageRouter<T> router, @NotNull CradleManager cradleManager) {
        super(router, cradleManager);
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
            AtomicLong lastSeqHolder = sessionToLastSequence.computeIfAbsent(sessionKey, ignore -> new AtomicLong(Long.MIN_VALUE));
            long prevLastSeq = lastSeqHolder.getAndAccumulate(lastSequence, Math::max);
            if (prevLastSeq >= firstSequence) {
                logger.error("Duplicated batch found: {}", messageBatch);
                return;
            }
            storeMessages(messages);
        } catch (Exception ex) {
            logger.error("Cannot handle delivery of type {}", messageBatch.getClass(), ex);
        }
    }

    @Override
    public void dispose() {
        super.dispose();

        logger.debug("Waiting for futures completion");
        Collection<CompletableFuture<Void>> futuresToRemove = new HashSet<>();
        while (!(asyncStoreFutures.isEmpty() || Thread.currentThread().isInterrupted())) {
            logger.info("Wait for the completion of {} futures", asyncStoreFutures.size());
            futuresToRemove.clear();
            asyncStoreFutures.forEach((future, batch) -> {
                try {
                    if (!future.isCancelled()) {
                        future.get(1, TimeUnit.SECONDS);
                    }
                    futuresToRemove.add(future);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                    Thread.currentThread().interrupt();
                } catch (CancellationException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("{} - future related to {} batch is cancelled", getClass().getSimpleName(), formatStoredMessageBatch(batch, false), e);
                    }
                } catch (ExecutionException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("{} - storing {} batch is failure", getClass().getSimpleName(), formatStoredMessageBatch(batch, false), e);
                    }
                } catch (TimeoutException e) {
                    if (logger.isErrorEnabled()) {
                        logger.error("{} - future related to {} batch can't be complited", getClass().getSimpleName(), formatStoredMessageBatch(batch, false), e);
                    }
                    future.cancel(false);
                }
            });
            asyncStoreFutures.keySet().removeAll(futuresToRemove);
        }
    }

    protected void storeMessages(List<M> messagesList) throws CradleStorageException {
        logger.debug("Process {} messages started, max {}", messagesList.size(), MAX_MESSAGES_COUNT);

        StoredMessageBatch storedMessageBatch = new StoredMessageBatch();
        for (M message : messagesList) {
            MessageToStore messageToStore = convert(message);
            storedMessageBatch.addMessage(messageToStore);
        }
        CompletableFuture<Void> future = store(storedMessageBatch);
        asyncStoreFutures.put(future, storedMessageBatch);
        future.whenCompleteAsync((value, exception) -> {
            try {
                if (exception == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} - batch stored: {}", getClass().getSimpleName(), formatStoredMessageBatch(storedMessageBatch, true));
                    }
                } else {
                    if (logger.isErrorEnabled()) {
                        logger.error("{} - batch storing is failure: {}", getClass().getSimpleName(), formatStoredMessageBatch(storedMessageBatch, true), exception);
                    }
                }
            } finally {
                if(asyncStoreFutures.remove(future) == null) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("{} - future related to batch {} already removed", getClass().getSimpleName(), formatStoredMessageBatch(storedMessageBatch, true));
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
            builder.append("size", storedMessageBatch.getMessageCount())
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
