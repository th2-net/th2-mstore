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
import static com.exactpro.th2.common.event.EventUtils.toTimestamp;
import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static org.junit.jupiter.api.Assertions.*;
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
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import com.exactpro.cradle.CradleEntitiesFactory;
import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.messages.MessageBatchToStore;
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
    private static final int TEST_MESSAGE_BATCH_SIZE = 1024;
    private static final int TEST_EVENT_BATCH_SIZE = CradleStorage.DEFAULT_MAX_MESSAGE_BATCH_SIZE;

    private final CradleManager cradleManagerMock = mock(CradleManager.class);
    private final CradleStorage storageMock = mock(CradleStorage.class);
    @SuppressWarnings("unchecked")
    private final MessageRouter<T> routerMock = (MessageRouter<T>) mock(MessageRouter.class);
    private final CradleStoreFunction storeFunction;
    private final Random random = new Random();

    @SuppressWarnings("unchecked")
    private final CompletableFuture<Void> completableFuture = mock(CompletableFuture.class);

    private AbstractMessageStore<T, M> messageStore;

    private CradleEntitiesFactory cradleEntitiesFactory;

    protected TestCaseMessageStore(CradleStoreFunction storeFunction) {
        this.storeFunction = storeFunction;
    }

    @BeforeEach
    void setUp() throws CradleStorageException, IOException {
        cradleEntitiesFactory = spy(new CradleEntitiesFactory(TEST_MESSAGE_BATCH_SIZE, CradleStorage.DEFAULT_MAX_MESSAGE_BATCH_DURATION_LIMIT_SECONDS, TEST_EVENT_BATCH_SIZE));

        when(storageMock.getEntitiesFactory()).thenReturn(cradleEntitiesFactory);
        when(storageMock.storeGroupedMessageBatchAsync(any(MessageBatchToStore.class), any(String.class))).thenReturn(completableFuture);

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

    protected abstract M createMessage(String sessionAlias, String sessionGroup, Direction direction, long sequence, String bookName);

    protected abstract long extractSize(M message);

    protected abstract T createDelivery(List<M> messages);

    protected abstract Timestamp extractTimestamp(M message);

    @NotNull
    protected MessageID createMessageId(Instant timestamp, String sessionAlias, String sessionGroup, Direction direction, long sequence, String bookName) {
        return MessageID.newBuilder()
                .setTimestamp(toTimestamp(timestamp))
                .setConnectionId(ConnectionID.newBuilder().setSessionAlias(sessionAlias).setSessionGroup(sessionGroup).build())
                .setDirection(direction)
                .setSequence(sequence)
                .setBookName(bookName)
                .build();
    }

    private static Instant from(TimestampOrBuilder timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    private static String bookName(int i) {
        return "book-name-" + i;
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private T deliveryOf(M... messages) {
        return createDelivery(List.of(messages));
    }

    private static void assertMessageBatchToStore(MessageBatchToStore batch, String bookName, String sessionAlias, Direction direction, int count) {
        assertEquals(bookName, batch.getId().getBookId().getName());
        assertEquals(sessionAlias, batch.getSessionAlias());
        assertEquals(toCradleDirection(direction), batch.getDirection());
        assertEquals(count, batch.getMessageCount());
    }

    private static void assertAllGroupValuesMatchTo(ArgumentCaptor<String> capture, String value, int count) {
        List<String> values = capture.getAllValues();
        if (count == 0) {
            if (! (values == null || values.size() == 0))
                Assertions.fail("Expecting empty values for groups");
            return;
        }

        assertNotNull(values);
        assertEquals(count, values.size());
        values.forEach(group -> {
            if (value == null)
                assertNull(group);
            else
                assertEquals(value, group);
        });
    }

    @Nested
    @DisplayName("Incorrect delivery content")
    class TestNegativeCases {

        @Test
        @DisplayName("Empty delivery is not stored")
        void testEmptyDelivery() throws CradleStorageException, IOException {
            messageStore.handle(deliveryOf());
            verify(messageStore, never()).storeMessages(any(), any());
        }

        @Test
        @DisplayName("Delivery with unordered sequences is not stored")
        void testUnorderedDelivery() throws CradleStorageException, IOException {
            String bookName = bookName(random.nextInt());
            M first = createMessage("test", "group", Direction.FIRST, 1, bookName);
            M second = createMessage("test", "group",  Direction.FIRST, 2, bookName);

            messageStore.handle(deliveryOf(second, first));
            verify(messageStore, never()).storeMessages(any(), any());
        }

        @Test
        @DisplayName("Delivery with different aliases is not stored")
        void testDifferentAliases() throws CradleStorageException, IOException {
            String bookName = bookName(random.nextInt());
            M first = createMessage("testA", "group", Direction.FIRST, 1, bookName);
            M second = createMessage("testB", "group", Direction.FIRST, 2, bookName);

            messageStore.handle(deliveryOf(first, second));
            verify(messageStore, never()).storeMessages(any(), any());
        }

        @Test
        @DisplayName("Delivery with different directions is not stored")
        void testDifferentDirections() throws CradleStorageException, IOException {
            String bookName = bookName(random.nextInt());
            M first = createMessage("test", "group", Direction.FIRST, 1, bookName);
            M second = createMessage("test", "group", Direction.SECOND, 2, bookName);

            messageStore.handle(deliveryOf(first, second));
            verify(messageStore, never()).storeMessages(any(), any());
        }

        @Test
        @DisplayName("Duplicated delivery is ignored")
        void testDuplicatedDelivery() throws CradleStorageException, IOException {
            String bookName = bookName(random.nextInt());

            M first = createMessage("test", "group", Direction.FIRST, 1, bookName);
            messageStore.handle(deliveryOf(first));

            M duplicate = createMessage("test", "group", Direction.FIRST, 1, bookName);
            messageStore.handle(deliveryOf(duplicate));

            ArgumentCaptor<MessageBatchToStore> batchCapture = ArgumentCaptor.forClass(MessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), batchCapture.capture(), groupCapture.capture());
            verify(cradleEntitiesFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/)).messageBatch();

            assertEquals(1, batchCapture.getAllValues().size());

            MessageBatchToStore batchValue = batchCapture.getValue();
            assertNotNull(batchValue);
            assertMessageBatchToStore(batchValue, bookName, "test", Direction.FIRST, 1);
            assertEquals(from(extractTimestamp(first)), batchValue.getLastTimestamp());

            assertAllGroupValuesMatchTo(groupCapture, "group", 1);
        }
    }

    @Nested
    @DisplayName("Handling single delivery")
    class TestSingleDelivery {

        @Test
        @DisplayName("Different sessions can have the same sequence")
        void testDifferentDirectionDelivery() throws CradleStorageException, IOException {
            String bookName = bookName(random.nextInt());
            
            M first = createMessage("testA", "group", Direction.FIRST, 1, bookName);
            messageStore.handle(deliveryOf(first));

            M duplicate = createMessage("testB", "group", Direction.SECOND, 1, bookName);
            messageStore.handle(deliveryOf(duplicate));

            ArgumentCaptor<MessageBatchToStore> batchCapture = ArgumentCaptor.forClass(MessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT).times(2)), batchCapture.capture(), groupCapture.capture());
            int invocations = 2 + 2/*two sessions*/ * 2 /*invocations in SessionBatchHolder (init + reset)*/;
            verify(cradleEntitiesFactory, times(invocations)).messageBatch();

            List<MessageBatchToStore> value = batchCapture.getAllValues();
            assertNotNull(value);
            assertEquals(2, value.size());

            MessageBatchToStore firstValue = value.stream()
                    .filter(it -> it.getDirection() == toCradleDirection(Direction.FIRST))
                    .findFirst().orElseThrow();
            assertMessageBatchToStore(firstValue, bookName, "testA", Direction.FIRST, 1);

            MessageBatchToStore secondValue = value.stream()
                    .filter(it -> it.getDirection() == toCradleDirection(Direction.SECOND))
                    .findFirst().orElseThrow();
            assertMessageBatchToStore(secondValue, bookName, "testB", Direction.SECOND, 1);

            assertAllGroupValuesMatchTo(groupCapture, "group", 2);
        }

        @Test
        @DisplayName("Delivery with single message is stored normally")
        void testSingleMessageDelivery() throws CradleStorageException, IOException {
            String bookName = bookName(random.nextInt());
            M first = createMessage("test", "group", Direction.FIRST, 1, bookName);

            messageStore.handle(deliveryOf(first));

            ArgumentCaptor<MessageBatchToStore> batchCapture = ArgumentCaptor.forClass(MessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), batchCapture.capture(), groupCapture.capture());
            verify(cradleEntitiesFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/)).messageBatch();

            MessageBatchToStore value = batchCapture.getValue();
            assertNotNull(value);
            assertMessageBatchToStore(value, bookName, "test", Direction.FIRST, 1);

            assertAllGroupValuesMatchTo(groupCapture, "group", 1);
        }

        @Test
        @DisplayName("Delivery with ordered messages for one session are stored")
        void testNormalDelivery() throws CradleStorageException, IOException {
            String bookName = bookName(random.nextInt());
            M first = createMessage("test", "group", Direction.FIRST, 1, bookName);
            M second = createMessage("test", "group", Direction.FIRST, 2, bookName);

            messageStore.handle(deliveryOf(first, second));

            ArgumentCaptor<MessageBatchToStore> batchCapture = ArgumentCaptor.forClass(MessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), batchCapture.capture(), groupCapture.capture());
            verify(cradleEntitiesFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/)).messageBatch();

            MessageBatchToStore value = batchCapture.getValue();
            assertNotNull(value);
            assertMessageBatchToStore(value, bookName, "test", Direction.FIRST, 2);

            assertAllGroupValuesMatchTo(groupCapture, "group", 1);
        }
    }

    @Nested
    @DisplayName("Several deliveries for one session")
    class TestSeveralDeliveriesInOneSession {
        @Test
        @DisplayName("Delivery for the same session ara joined to one batch")
        void joinsBatches() throws IOException, CradleStorageException {
            String bookName = bookName(random.nextInt());
            M first = createMessage("test", "group", Direction.FIRST, 1, bookName);
            M second = createMessage("test", "group", Direction.FIRST, 2, bookName);

            messageStore.handle(deliveryOf(first));
            messageStore.handle(deliveryOf(second));

            ArgumentCaptor<MessageBatchToStore> batchCapture = ArgumentCaptor.forClass(MessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), batchCapture.capture(), groupCapture.capture());
            verify(cradleEntitiesFactory, times(2 + 2/*invocations in SessionBatchHolder (init + reset)*/)).messageBatch();

            MessageBatchToStore value = batchCapture.getValue();
            assertNotNull(value);
            assertMessageBatchToStore(value, bookName, "test", Direction.FIRST, 2);

            assertAllGroupValuesMatchTo(groupCapture, "group", 1);
        }

        /**
         * Message size calculation changed in Cradle, please see
         * {@link com.exactpro.cradle.serialization.MessagesSizeCalculator#calculateMessageSizeInBatch(com.exactpro.cradle.messages.CradleMessage)}
         * {@link com.exactpro.cradle.serialization.MessagesSizeCalculator#calculateMessageSize(com.exactpro.cradle.messages.CradleMessage)}
         */
        @Test
        @DisplayName("Stores batch if cannot join because of messages size")
        void storesBatch() throws IOException, CradleStorageException {
            String bookName = bookName(random.nextInt());
            long oneMessageSize = extractSize(createMessage("test", "group", Direction.FIRST, 1, bookName))
                    + MESSAGE_LENGTH_IN_BATCH
                    + MESSAGE_SIZE_CONST_VALUE;
            long maxMessagesInBatchCount = TEST_MESSAGE_BATCH_SIZE / oneMessageSize;
            List<M> firstDelivery = LongStream.range(0, maxMessagesInBatchCount / 2)
                    .mapToObj(it -> createMessage("test", "group", Direction.FIRST, it, bookName))
                    .collect(Collectors.toList());

            List<M> secondDelivery = LongStream.range(maxMessagesInBatchCount / 2, maxMessagesInBatchCount + maxMessagesInBatchCount / 2)
                    .mapToObj(it -> createMessage("test", "group", Direction.FIRST, it, bookName))
                    .collect(Collectors.toList());

            messageStore.handle(createDelivery(firstDelivery));
            messageStore.handle(createDelivery(secondDelivery));

            ArgumentCaptor<MessageBatchToStore> batchCapture = ArgumentCaptor.forClass(MessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT).times(2)), batchCapture.capture(), groupCapture.capture());
            verify(cradleEntitiesFactory, times(2 + 2/*invocations in SessionBatchHolder (init + reset)*/)).messageBatch();

            List<MessageBatchToStore> value = batchCapture.getAllValues();
            assertNotNull(value);
            assertEquals(2, value.size());
            assertMessageBatchToStore(value.get(0), bookName, "test", Direction.FIRST, firstDelivery.size());
            assertMessageBatchToStore(value.get(1), bookName, "test", Direction.FIRST, secondDelivery.size());

            assertAllGroupValuesMatchTo(groupCapture, "group", 2);

        }
    }

    @Nested
    @DisplayName("Deliveries for different groups")
    class TestDeliveriesForDifferentGroups {
        @Test
        @DisplayName("Deliveries for different groups are stored separately")
        void separateBatches() throws IOException, CradleStorageException {
            String bookName = bookName(random.nextInt());
            M first = createMessage("testA", "group1", Direction.FIRST, 1, bookName);
            M second = createMessage("testB", "group2", Direction.FIRST, 2, bookName);

            messageStore.handle(deliveryOf(first));
            messageStore.handle(deliveryOf(second));

            ArgumentCaptor<MessageBatchToStore> batchCapture = ArgumentCaptor.forClass(MessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT).times(2)), batchCapture.capture(), groupCapture.capture());
            verify(cradleEntitiesFactory, times(2 + 4/*invocations in SessionBatchHolder (init + reset)*/)).messageBatch();

            List<MessageBatchToStore> batchValues = batchCapture.getAllValues();
            assertNotNull(batchValues);
            assertEquals(2, batchValues.size());

            List<String> groupValues = groupCapture.getAllValues();
            assertNotNull(groupValues);
            assertEquals(2, groupValues.size());

            for (int i = 0; i < groupValues.size(); i++) {
                String group = groupValues.get(i);
                MessageBatchToStore batch = batchValues.get(i);
                if (group.equals("group1")) {
                    assertMessageBatchToStore(batch, bookName, "testA", Direction.FIRST, 1);
                    assertEquals(1, batch.getId().getSequence());
                }
                if (group.equals("group2")) {
                    assertMessageBatchToStore(batch, bookName, "testB", Direction.FIRST, 1);
                    assertEquals(2, batch.getId().getSequence());
                }
            }
        }
    }

    @Test
    @DisplayName("Close message store when feature is completed")
    void testCompletedFutureCompleted() throws InterruptedException, ExecutionException, TimeoutException {
        M first = createMessage("test", "group", Direction.FIRST, 1, bookName(random.nextInt()));

        messageStore.handle(deliveryOf(first));
        messageStore.dispose();

        verify(completableFuture).get(any(long.class), any(TimeUnit.class));
    }

    @Test
    @DisplayName("Close message store when feature throws TimeoutException")
    void testCompletedFutureTimeoutException() throws InterruptedException, ExecutionException, TimeoutException {
        when(completableFuture.get(any(long.class), any(TimeUnit.class))).thenThrow(TimeoutException.class);
        when(completableFuture.isDone()).thenReturn(false, true);
        when(completableFuture.cancel(any(boolean.class))).thenReturn(false);

        M first = createMessage("test", "group", Direction.FIRST, 1, bookName(random.nextInt()));

        messageStore.handle(deliveryOf(first));
        messageStore.dispose();

        verify(completableFuture).get(any(long.class), any(TimeUnit.class));
        verify(completableFuture, times(2)).isDone();
        verify(completableFuture).cancel(false);
    }

    @Test
    @DisplayName("Close message store when feature throws InterruptedException")
    void testCompletedFutureInterruptedException() throws InterruptedException, ExecutionException, TimeoutException {
        when(completableFuture.get(any(long.class), any(TimeUnit.class))).thenThrow(InterruptedException.class);
        when(completableFuture.isDone()).thenReturn(false, true);
        when(completableFuture.cancel(any(boolean.class))).thenReturn(false);

        M first = createMessage("test", "group", Direction.FIRST, 1, bookName(random.nextInt()));

        messageStore.handle(deliveryOf(first));
        messageStore.dispose();

        verify(completableFuture).get(any(long.class), any(TimeUnit.class));
        verify(completableFuture).isDone();
        verify(completableFuture).cancel(true);
    }

    @Test
    @DisplayName("Close message store when feature throws ExecutionException")
    void testCompletedFutureExecutionException() throws InterruptedException, ExecutionException, TimeoutException {
        when(completableFuture.get(any(long.class), any(TimeUnit.class))).thenThrow(ExecutionException.class);

        M first = createMessage("test", "group", Direction.FIRST, 1, bookName(random.nextInt()));

        messageStore.handle(deliveryOf(first));
        messageStore.dispose();

        verify(completableFuture).get(any(long.class), any(TimeUnit.class));
        verify(completableFuture).isDone();
    }

    protected interface CradleStoreFunction {
        CompletableFuture<Void> store(CradleStorage storage, MessageBatchToStore batch, String sessionGroup) throws CradleStorageException, IOException;
    }
}