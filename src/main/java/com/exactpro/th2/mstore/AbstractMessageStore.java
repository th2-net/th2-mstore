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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.jetbrains.annotations.NotNull;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.store.common.AbstractStorage;

public abstract class AbstractMessageStore<T, M> extends AbstractStorage<T> {
    private final Map<SessionKey, AtomicLong> sessionToLastSequence = new ConcurrentHashMap<>();

    public AbstractMessageStore(MessageRouter<T> router, @NotNull CradleManager cradleManager) {
        super(router, cradleManager);
    }

    @Override
    public final void handle(T delivery) {
        try {
            verifyDelivery(delivery);
            List<M> messages = getMessages(delivery);
            if (messages.isEmpty()) {
                logger.warn("Empty batch has been received"); //FIXME: need identify
                return;
            }
            M lastMessage = messages.get(messages.size() - 1);
            long firstSequence = extractSequence(messages.get(0));
            long lastSequence = extractSequence(lastMessage);
            SessionKey sessionKey = createSessionKey(lastMessage);
            AtomicLong lastSeqHolder = sessionToLastSequence.computeIfAbsent(sessionKey, ignore -> new AtomicLong(Long.MIN_VALUE));
            long prevLastSeq = lastSeqHolder.getAndAccumulate(lastSequence, Math::max);
            if (prevLastSeq >= firstSequence) {
                logger.error("Duplicated delivery found: {}", delivery);
                return;
            }
            storeMessages(messages);
        } catch (Exception ex) {
            logger.error("Cannot handle delivery of type {}", delivery.getClass(), ex);
        }
    }

    protected void storeMessages(List<M> messagesList) throws CradleStorageException, IOException {
        logger.debug("Process {} messages started, max {}", messagesList.size(), MAX_MESSAGES_COUNT);

        StoredMessageBatch storedMessageBatch = new StoredMessageBatch();
        for (M message : messagesList) {
            MessageToStore messageToStore = convert(message);
            storedMessageBatch.addMessage(messageToStore);
        }
        store(getCradleManager(), storedMessageBatch);
        logger.debug("Message Batch stored: stream '{}', direction '{}', id '{}', size '{}', messages '{}'",
                storedMessageBatch.getStreamName(), storedMessageBatch.getId().getDirection(), storedMessageBatch.getId().getIndex(),
                storedMessageBatch.getMessageCount(), storedMessageBatch.getMessages());
    }

    /**
     * Checks that the delivery contains all messages related to one session
     * and that each message has sequence number greater than the previous one.
     * @param delivery the delivery received from router
     */
    private void verifyDelivery(T delivery) {
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

    private void verifySequence(int messageIndex, long lastSeq, long currentSeq) {
        if (lastSeq >= currentSeq) {
            throw new IllegalArgumentException(
                    String.format(
                            "Delivery contains unordered messages. Message [%d] - seqN %d; Message [%d] - seqN %d",
                            messageIndex - 1, lastSeq, messageIndex, currentSeq
                    )
            );
        }
    }

    private void verifySession(int messageIndex, SessionKey lastKey, SessionKey sessionKey) {
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

    protected abstract void store(CradleManager cradleManager, StoredMessageBatch storedMessageBatch) throws IOException;

    protected abstract long extractSequence(M message);

    protected abstract SessionKey createSessionKey(M message);

    @FunctionalInterface
    protected interface CradleStoredMessageBatchFunction {
        void store(StoredMessageBatch storedMessageBatch) throws IOException;
    }

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
        public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SessionKey that = (SessionKey)o;

            return new EqualsBuilder()
                    .append(streamName, that.streamName)
                    .append(direction, that.direction)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(streamName)
                    .append(direction)
                    .toHashCode();
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this, ToStringStyle.NO_CLASS_NAME_STYLE)
                    .append("streamName", streamName)
                    .append("direction", direction)
                    .toString();
        }
    }
}
