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

import static com.exactpro.cradle.serialization.MessagesSizeCalculator.MESSAGE_LENGTH_IN_BATCH;
import static com.exactpro.cradle.serialization.MessagesSizeCalculator.MESSAGE_SIZE_CONST_VALUE;
import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleObjectsFactory;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.mstore.cfg.MessageStoreConfiguration;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampOrBuilder;

abstract class TestCaseMessageStore<T extends GeneratedMessageV3, M extends GeneratedMessageV3> {

    private static final int DRAIN_TIMEOUT = 1000;
    private static final long TEST_MESSAGE_BATCH_SIZE = 1024;
    private static final long TEST_EVENT_BATCH_SIZE = StoredTestEventBatch.DEFAULT_MAX_BATCH_SIZE;

    private final CradleManager cradleManagerMock = mock(CradleManager.class);

    private final CradleStorage storageMock = mock(CradleStorage.class);

    @SuppressWarnings("unchecked")
    private final MessageRouter<T> routerMock = (MessageRouter<T>)mock(MessageRouter.class);

    private final CradleStoreFunction storeFunction;

    @SuppressWarnings("unchecked")
    private final CompletableFuture<Void> completableFuture = mock(CompletableFuture.class);

    private AbstractMessageStore<T, M> messageStore;

    private CradleObjectsFactory cradleObjectsFactory;

    protected TestCaseMessageStore(CradleStoreFunction storeFunction) {
        this.storeFunction = storeFunction;
    }

    @BeforeEach
    void setUp() {
        cradleObjectsFactory = spy(new CradleObjectsFactory(TEST_MESSAGE_BATCH_SIZE, TEST_EVENT_BATCH_SIZE));

        when(storageMock.getObjectsFactory()).thenReturn(cradleObjectsFactory);
        when(storageMock.storeProcessedMessageBatchAsync(any(StoredMessageBatch.class))).thenReturn(completableFuture);
        when(storageMock.storeMessageBatchAsync(any(StoredMessageBatch.class))).thenReturn(completableFuture);

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

    protected abstract long extractSize(M message);

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

    private static Instant from(TimestampOrBuilder timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private T deliveryOf(M... messages) {
        return createDelivery(List.of(messages));
    }

    private static void assertStoredMessageBatch(StoredMessageBatch batch, String streamName, Direction direction, int count) {
        assertEquals(toCradleDirection(direction), batch.getDirection());
        assertEquals(streamName, batch.getStreamName());
        assertEquals(count, batch.getMessageCount());
    }

    @Nested
    @DisplayName("Incorrect delivery content")
    class TestNegativeCases {

        @Test
        @DisplayName("Empty delivery is not stored")
        void testEmptyDelivery() throws CradleStorageException {
            messageStore.handle(deliveryOf());
            verify(messageStore, never()).storeMessages(any(), any());
        }

        @Test
        @DisplayName("Delivery with unordered sequences is not stored")
        void testUnorderedDelivery() throws CradleStorageException {
            M first = createMessage("test", Direction.FIRST, 1);
            M second = createMessage("test", Direction.FIRST, 2);

            messageStore.handle(deliveryOf(second, first));
            verify(messageStore, never()).storeMessages(any(), any());
        }

        @Test
        @DisplayName("Delivery with different aliases is not stored")
        void testDifferentAliases() throws CradleStorageException {
            M first = createMessage("testA", Direction.FIRST, 1);
            M second = createMessage("testB", Direction.FIRST, 2);

            messageStore.handle(deliveryOf(first, second));
            verify(messageStore, never()).storeMessages(any(), any());
        }

        @Test
        @DisplayName("Delivery with different directions is not stored")
        void testDifferentDirections() throws CradleStorageException {
            M first = createMessage("test", Direction.FIRST, 1);
            M second = createMessage("test", Direction.SECOND, 2);

            messageStore.handle(deliveryOf(first, second));
            verify(messageStore, never()).storeMessages(any(), any());
        }

        @Test
        @DisplayName("Duplicated delivery is ignored")
        void testDuplicatedDelivery() {
            M first = createMessage("test", Direction.FIRST, 1);
            messageStore.handle(deliveryOf(first));

            M duplicate = createMessage("test", Direction.FIRST, 1);
            messageStore.handle(deliveryOf(duplicate));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), capture.capture());
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createMessageBatch();

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
        void testDifferentDirectionDelivery() {
            M first = createMessage("testA", Direction.FIRST, 1);
            messageStore.handle(deliveryOf(first));

            M duplicate = createMessage("testB", Direction.SECOND, 1);
            messageStore.handle(deliveryOf(duplicate));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT).times(2)), capture.capture());
            int invocations = 2 + 2/*two sessions*/ * 2 /*invocations in SessionBatchHolder (init + reset)*/;
            verify(cradleObjectsFactory, times(invocations)).createMessageBatch();

            List<StoredMessageBatch> value = capture.getAllValues();
            assertNotNull(value);
            assertEquals(2, value.size());

            StoredMessageBatch firstValue = value.stream()
                    .filter(it -> it.getDirection() == toCradleDirection(Direction.FIRST))
                    .findFirst().orElseThrow();
            assertStoredMessageBatch(firstValue, "testA", Direction.FIRST, 1);

            StoredMessageBatch secondValue = value.stream()
                    .filter(it -> it.getDirection() == toCradleDirection(Direction.SECOND))
                    .findFirst().orElseThrow();
            assertStoredMessageBatch(secondValue, "testB", Direction.SECOND, 1);
        }

        @Test
        @DisplayName("Delivery with single message is stored normally")
        void testSingleMessageDelivery() {
            M first = createMessage("test", Direction.FIRST, 1);

            messageStore.handle(deliveryOf(first));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), capture.capture());
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createMessageBatch();

            StoredMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessageBatch(value, "test", Direction.FIRST, 1);
        }

        @Test
        @DisplayName("Delivery with ordered messages for one session are stored")
        void testNormalDelivery() {
            M first = createMessage("test", Direction.FIRST, 1);
            M second = createMessage("test", Direction.FIRST, 2);

            messageStore.handle(deliveryOf(first, second));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), capture.capture());
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createMessageBatch();

            StoredMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessageBatch(value, "test", Direction.FIRST, 2);
        }
    }

    @Nested
    @DisplayName("Several deliveries for one session")
    class TestSeveralDeliveriesInOneSession {
        @Test
        @DisplayName("Delivery for the same session ara joined to one batch")
        void joinsBatches() throws IOException {
            M first = createMessage("test", Direction.FIRST, 1);
            M second = createMessage("test", Direction.FIRST, 2);

            messageStore.handle(deliveryOf(first));
            messageStore.handle(deliveryOf(second));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), capture.capture());
            verify(cradleObjectsFactory, times(2 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createMessageBatch();

            StoredMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessageBatch(value, "test", Direction.FIRST, 2);
        }

        /**
         * Message size calculation changed in Cradle, please see
         * {@link com.exactpro.cradle.serialization.MessagesSizeCalculator#calculateMessageSizeInBatch(com.exactpro.cradle.messages.MessageToStore)}
         * {@link com.exactpro.cradle.serialization.MessagesSizeCalculator#calculateMessageSize(com.exactpro.cradle.messages.MessageToStore)}
         */
        @Test
        @DisplayName("Stores batch if cannot join because of messages size")
        void storesBatch() throws IOException {
            long oneMessageSize = extractSize(createMessage("test", Direction.FIRST, 1))
                    + MESSAGE_LENGTH_IN_BATCH
                    + MESSAGE_SIZE_CONST_VALUE;
            long maxMessagesInBatchCount = TEST_MESSAGE_BATCH_SIZE / oneMessageSize;
            List<M> firstDelivery = LongStream.range(0, maxMessagesInBatchCount / 2)
                    .mapToObj(it -> createMessage("test", Direction.FIRST, it))
                    .collect(Collectors.toList());

            List<M> secondDelivery = LongStream.range(maxMessagesInBatchCount / 2, maxMessagesInBatchCount + maxMessagesInBatchCount / 2)
                    .mapToObj(it -> createMessage("test", Direction.FIRST, it))
                    .collect(Collectors.toList());

            messageStore.handle(createDelivery(firstDelivery));
            messageStore.handle(createDelivery(secondDelivery));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT * 2).times(2)), capture.capture());
            verify(cradleObjectsFactory, times(2 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createMessageBatch();

            List<StoredMessageBatch> value = capture.getAllValues();
            assertNotNull(value);
            assertEquals(2, value.size());
            assertStoredMessageBatch(value.get(0), "test", Direction.FIRST, firstDelivery.size());
            assertStoredMessageBatch(value.get(1), "test", Direction.FIRST, secondDelivery.size());
        }
    }

    @Test
    @DisplayName("Close message store when feature is complited")
    void testComplitedFutureComplited() throws InterruptedException, ExecutionException, TimeoutException {
        M first = createMessage("test", Direction.FIRST, 1);

        messageStore.handle(deliveryOf(first));
        messageStore.dispose();

        verify(completableFuture).get(any(long.class), any(TimeUnit.class));
    }

    @Test
    @DisplayName("Close message store when feature throws TimeoutException")
    void testComplitedFutureTimeoutException() throws InterruptedException, ExecutionException, TimeoutException {
        when(completableFuture.get(any(long.class), any(TimeUnit.class))).thenThrow(TimeoutException.class);
        when(completableFuture.isDone()).thenReturn(false, true);
        when(completableFuture.cancel(any(boolean.class))).thenReturn(false);

        M first = createMessage("test", Direction.FIRST, 1);

        messageStore.handle(deliveryOf(first));
        messageStore.dispose();

        verify(completableFuture).get(any(long.class), any(TimeUnit.class));
        verify(completableFuture, times(2)).isDone();
        verify(completableFuture).cancel(false);
    }

    @Test
    @DisplayName("Close message store when feature throws InterruptedException")
    void testComplitedFutureInterruptedException() throws InterruptedException, ExecutionException, TimeoutException {
        when(completableFuture.get(any(long.class), any(TimeUnit.class))).thenThrow(InterruptedException.class);
        when(completableFuture.isDone()).thenReturn(false, true);
        when(completableFuture.cancel(any(boolean.class))).thenReturn(false);

        M first = createMessage("test", Direction.FIRST, 1);

        messageStore.handle(deliveryOf(first));
        messageStore.dispose();

        verify(completableFuture).get(any(long.class), any(TimeUnit.class));
        verify(completableFuture).isDone();
        verify(completableFuture).cancel(true);
    }

    @Test
    @DisplayName("Close message store when feature throws ExecutionException")
    void testComplitedFutureExecutionException() throws InterruptedException, ExecutionException, TimeoutException {
        when(completableFuture.get(any(long.class), any(TimeUnit.class))).thenThrow(ExecutionException.class);

        M first = createMessage("test", Direction.FIRST, 1);

        messageStore.handle(deliveryOf(first));
        messageStore.dispose();

        verify(completableFuture).get(any(long.class), any(TimeUnit.class));
        verify(completableFuture).isDone();
    }

    protected interface CradleStoreFunction {
        CompletableFuture<Void> store(CradleStorage storage, StoredMessageBatch batch);
    }
}