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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.mstore.cfg.MessageStoreConfiguration;
import com.exactpro.th2.store.common.utils.ProtoUtil;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampOrBuilder;

abstract class TestCaseMessageStore<T, M> {

    private static final int DRAIN_TIMEOUT = 1000;

    private CradleStorage storageMock;

    private final CradleStoreFunction storeFunction;

    private AbstractMessageStore<T, M> messageStore;

    protected TestCaseMessageStore(CradleStoreFunction storeFunction) {
        this.storeFunction = storeFunction;
    }

    @BeforeEach
    void setUp() {
        CradleManager cradleManagerMock = mock(CradleManager.class);
        storageMock = mock(CradleStorage.class);
        //noinspection unchecked
        MessageRouter<T> routerMock = (MessageRouter<T>)mock(MessageRouter.class);

        when(cradleManagerMock.getStorage()).thenReturn(storageMock);
        when(routerMock.subscribeAll(any(), any())).thenReturn(mock(SubscriberMonitor.class));
        MessageStoreConfiguration configuration = new MessageStoreConfiguration();
        configuration.setDrainInterval(DRAIN_TIMEOUT / 10);
        messageStore = spy(createStore(cradleManagerMock, routerMock, configuration));
        messageStore.start();
    }

    @AfterEach
    void tearDown() {
        messageStore.dispose();
    }

    protected abstract AbstractMessageStore<T, M> createStore(CradleManager cradleManagerMock, MessageRouter<T> routerMock, MessageStoreConfiguration configuration);

    protected abstract M createMessage(String session, Direction direction, long sequence);

    protected abstract T createDelivery(List<M> messages);

    protected abstract Timestamp extractTimestamp(M message);

    @NotNull
    protected MessageID createMessageId(String session, Direction direction, long sequence) {
        return MessageID.newBuilder()
                .setDirection(direction)
                .setSequence(sequence)
                .setConnectionId(ConnectionID.newBuilder().setSessionAlias(session).build())
                .build();
    }

    protected Timestamp createTimestamp() {
        Instant now = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();
    }

    private Instant from(TimestampOrBuilder timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private T deliveryOf(M... messages) {
        return createDelivery(List.of(messages));
    }

    private void assertStoredMessageBatch(StoredMessageBatch batch, String streamName, Direction direction, int seq) {
        assertEquals(ProtoUtil.toCradleDirection(direction), batch.getDirection());
        assertEquals(streamName, batch.getStreamName());
        assertEquals(seq, batch.getMessageCount());
    }

    @Nested
    @DisplayName("Incorrect delivery content")
    class TestNegativeCases {

        @Test
        @DisplayName("Empty delivery is not stored")
        void emptyDelivery() throws IOException, CradleStorageException {
            messageStore.handle(deliveryOf());
            verify(messageStore, never()).storeMessages(any(), any());
        }

        @Test
        @DisplayName("Delivery with unordered sequences is not stored")
        void unorderedDelivery() throws IOException, CradleStorageException {
            M first = createMessage("test", Direction.FIRST, 1);
            M second = createMessage("test", Direction.FIRST, 2);

            messageStore.handle(deliveryOf(second, first));
            verify(messageStore, never()).storeMessages(any(), any());
        }

        @Test
        @DisplayName("Delivery with different aliases is not stored")
        void differentAliases() throws CradleStorageException, IOException {
            M first = createMessage("testA", Direction.FIRST, 1);
            M second = createMessage("testB", Direction.FIRST, 2);

            messageStore.handle(deliveryOf(first, second));
            verify(messageStore, never()).storeMessages(any(), any());
        }

        @Test
        @DisplayName("Delivery with different directions is not stored")
        void differentDirections() throws CradleStorageException, IOException {
            M first = createMessage("test", Direction.FIRST, 1);
            M second = createMessage("test", Direction.SECOND, 2);

            messageStore.handle(deliveryOf(first, second));
            verify(messageStore, never()).storeMessages(any(), any());
        }

        @Test
        @DisplayName("Duplicated delivery is ignored")
        void duplicatedDelivery() throws IOException {
            M first = createMessage("test", Direction.FIRST, 1);
            messageStore.handle(deliveryOf(first));

            M duplicate = createMessage("test", Direction.FIRST, 1);
            messageStore.handle(deliveryOf(duplicate));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), capture.capture());

            StoredMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessageBatch(value, "test", Direction.FIRST, 1);
            assertEquals(from(extractTimestamp(first)), value.getLastTimestamp());
        }
    }

    @Nested
    @DisplayName("Handling single delivery")
    class TestSingleDelivery {

        @Test
        @DisplayName("Different sessions can have the same sequence")
        void differentDirectionDelivery() throws IOException {
            M first = createMessage("testA", Direction.FIRST, 1);
            messageStore.handle(deliveryOf(first));

            M duplicate = createMessage("testB", Direction.SECOND, 1);
            messageStore.handle(deliveryOf(duplicate));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT).times(2)), capture.capture());

            List<StoredMessageBatch> value = capture.getAllValues();
            assertNotNull(value);
            assertEquals(2, value.size());

            StoredMessageBatch firstValue = value.stream()
                    .filter(it -> it.getDirection() == ProtoUtil.toCradleDirection(Direction.FIRST))
                    .findFirst().orElseThrow();
            assertStoredMessageBatch(firstValue, "testA", Direction.FIRST, 1);

            StoredMessageBatch secondValue = value.stream()
                    .filter(it -> it.getDirection() == ProtoUtil.toCradleDirection(Direction.SECOND))
                    .findFirst().orElseThrow();
            assertStoredMessageBatch(secondValue, "testB", Direction.SECOND, 1);
        }

        @Test
        @DisplayName("Delivery with single message is stored normally")
        void singleMessageDelivery() throws IOException {
            M first = createMessage("test", Direction.FIRST, 1);

            messageStore.handle(deliveryOf(first));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), capture.capture());

            StoredMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessageBatch(value, "test", Direction.FIRST, 1);
        }

        @Test
        @DisplayName("Delivery with ordered messages for one session are stored")
        void normalDelivery() throws IOException {
            M first = createMessage("test", Direction.FIRST, 1);
            M second = createMessage("test", Direction.FIRST, 2);

            messageStore.handle(deliveryOf(first, second));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), capture.capture());

            StoredMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessageBatch(value, "test", Direction.FIRST, 2);
        }
    }

    protected interface CradleStoreFunction {
        void store(CradleStorage storage, StoredMessageBatch batch) throws IOException;
    }
}