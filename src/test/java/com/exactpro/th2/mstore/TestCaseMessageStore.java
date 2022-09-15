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

import com.exactpro.cradle.CradleEntitiesFactory;
import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampOrBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.exactpro.th2.common.event.EventUtils.toTimestamp;
import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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
        cradleEntitiesFactory = spy(new CradleEntitiesFactory(TEST_MESSAGE_BATCH_SIZE, TEST_EVENT_BATCH_SIZE));

        when(storageMock.getEntitiesFactory()).thenReturn(cradleEntitiesFactory);
        when(storageMock.storeGroupedMessageBatchAsync(any(GroupedMessageBatchToStore.class))).thenReturn(completableFuture);
        when(storageMock.getLastSequence(any(), any(), any())).thenReturn(-1L);
        when(cradleManagerMock.getStorage()).thenReturn(storageMock);
        when(routerMock.subscribeAll(any(), any())).thenReturn(mock(SubscriberMonitor.class));
        Configuration configuration = Configuration.builder().withDrainInterval(DRAIN_TIMEOUT / 10).build();
        messageStore = spy(createStore(cradleManagerMock, routerMock, configuration));
        messageStore.start();
    }

    @AfterEach
    void tearDown() {
        messageStore.dispose();
    }

    protected abstract AbstractMessageStore<T, M> createStore(CradleManager cradleManagerMock, MessageRouter<T> routerMock, Configuration configuration);

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

    private static void assertMessageToStore(StoredMessage message, String bookName, String sessionAlias, Direction direction) {
        assertEquals(bookName, message.getBookId().getName());
        assertEquals(sessionAlias, message.getSessionAlias());
        assertEquals(toCradleDirection(direction), message.getDirection());
    }

    private static void assertMessageBatchToStore(GroupedMessageBatchToStore batch, String bookName, String groupName, int count) {
        assertEquals(bookName, batch.getBookId().getName());
        assertEquals(groupName, batch.getGroup());
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
        @DisplayName("Duplicated delivery is ignored")
        void testDuplicatedDelivery() throws CradleStorageException, IOException {
            String bookName = bookName(random.nextInt());

            M first = createMessage("test", "group", Direction.FIRST, 1, bookName);
            messageStore.handle(deliveryOf(first));

            M duplicate = createMessage("test", "group", Direction.FIRST, 1, bookName);
            messageStore.handle(deliveryOf(duplicate));

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), batchCapture.capture());
            verify(cradleEntitiesFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/))
                    .groupedMessageBatch(groupCapture.capture());

            assertEquals(1, batchCapture.getAllValues().size());

            GroupedMessageBatchToStore batchValue = batchCapture.getValue();
            assertNotNull(batchValue);
            assertMessageBatchToStore(batchValue, bookName, "group", 1);
            assertEquals(from(extractTimestamp(first)), batchValue.getLastTimestamp());
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

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT).times(1)), batchCapture.capture());
            int invocations = 2 +  2 /*invocations in SessionBatchHolder (init + reset)*/;
            verify(cradleEntitiesFactory, times(invocations)).groupedMessageBatch(groupCapture.capture());

            List<GroupedMessageBatchToStore> value = batchCapture.getAllValues();
            assertNotNull(value);
            assertEquals(1, value.size());

            GroupedMessageBatchToStore batch = value.get(0);

            boolean a = false, b = false;
            List<StoredMessage> messages = List.copyOf(batch.getMessages());
            assertEquals(2, messages.size());
            for (StoredMessage m : messages) {
                if (m.getDirection() == toCradleDirection(Direction.FIRST)) {
                    assertMessageToStore(m, bookName, "testA", Direction.FIRST);
                    a = true;
                } else {
                    assertMessageToStore(m, bookName, "testB", Direction.SECOND);
                    b = true;
                }
            }
            assertEquals(true, a);
            assertEquals(true, b);

            assertAllGroupValuesMatchTo(groupCapture, "group", 4);
        }

        @Test
        @DisplayName("Delivery with single message is stored normally")
        void testSingleMessageDelivery() throws CradleStorageException, IOException {
            String bookName = bookName(random.nextInt());
            M first = createMessage("test", "group", Direction.FIRST, 1, bookName);

            messageStore.handle(deliveryOf(first));

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), batchCapture.capture());
            verify(cradleEntitiesFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/))
                    .groupedMessageBatch(groupCapture.capture());

            GroupedMessageBatchToStore value = batchCapture.getValue();
            assertNotNull(value);
            assertEquals(1, value.getMessageCount());
            assertMessageToStore(List.copyOf(value.getMessages()).get(0), bookName, "test", Direction.FIRST);

            assertAllGroupValuesMatchTo(groupCapture, "group", 3);
        }

        @Test
        @DisplayName("Delivery with ordered messages for one session are stored")
        void testNormalDelivery() throws CradleStorageException, IOException {
            String bookName = bookName(random.nextInt());
            M first = createMessage("test", "group", Direction.FIRST, 1, bookName);
            M second = createMessage("test", "group", Direction.FIRST, 2, bookName);

            messageStore.handle(deliveryOf(first, second));

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), batchCapture.capture());
            verify(cradleEntitiesFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/))
                    .groupedMessageBatch(groupCapture.capture());

            GroupedMessageBatchToStore value = batchCapture.getValue();
            assertNotNull(value);
            assertEquals(2, value.getMessageCount());
            List<StoredMessage> messages = List.copyOf(value.getMessages());
            assertMessageToStore(messages.get(0), bookName, "test", Direction.FIRST);
            assertMessageToStore(messages.get(1), bookName, "test", Direction.FIRST);

            assertAllGroupValuesMatchTo(groupCapture, "group", 3);
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

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), batchCapture.capture());
            verify(cradleEntitiesFactory, times(2 + 2/*invocations in SessionBatchHolder (init + reset)*/))
                    .groupedMessageBatch(groupCapture.capture());

            GroupedMessageBatchToStore value = batchCapture.getValue();
            assertNotNull(value);
            assertEquals(2, value.getMessageCount());
            List<StoredMessage> messages = List.copyOf(value.getMessages());
            assertMessageToStore(messages.get(0), bookName, "test", Direction.FIRST);
            assertMessageToStore(messages.get(1), bookName, "test", Direction.FIRST);

            assertAllGroupValuesMatchTo(groupCapture, "group", 4);
        }

    }

    @Test
    @DisplayName("Delivery with different aliases is stored")
    void testDifferentAliases() throws CradleStorageException, IOException {
        String bookName = bookName(random.nextInt());
        M first = createMessage("testA", "group", Direction.FIRST, 1, bookName);
        M second = createMessage("testB", "group", Direction.FIRST, 2, bookName);

        messageStore.handle(deliveryOf(first, second));
        verify(messageStore, times(1)).storeMessages(any(), any());
    }

    @Test
    @DisplayName("Delivery with different directions is stored")
    void testDifferentDirections() throws CradleStorageException, IOException {
        String bookName = bookName(random.nextInt());
        M first = createMessage("test", "group", Direction.FIRST, 1, bookName);
        M second = createMessage("test", "group", Direction.SECOND, 2, bookName);

        messageStore.handle(deliveryOf(first, second));
        verify(messageStore, times(1)).storeMessages(any(), any());
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

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT).times(2)), batchCapture.capture());
            verify(cradleEntitiesFactory, times(2 + 4/*invocations in SessionBatchHolder (init + reset)*/))
                    .groupedMessageBatch(groupCapture.capture());

            List<GroupedMessageBatchToStore> batchValues = batchCapture.getAllValues();
            assertNotNull(batchValues);
            assertEquals(2, batchValues.size());

            List<String> groupValues = groupCapture.getAllValues();
            assertNotNull(groupValues);
            assertEquals(6, groupValues.size());

            for (int i = 0; i < batchValues.size(); i++) {
                GroupedMessageBatchToStore batch = batchValues.get(i);
                String group = batch.getGroup();
                if (group.equals("group1")) {
                    List<StoredMessage> messages = List.copyOf(batch.getMessages());
                    assertMessageToStore(messages.get(0), bookName, "testA", Direction.FIRST);
                    assertEquals(1, messages.get(0).getSequence());
                }
                if (group.equals("group2")) {
                    List<StoredMessage> messages = List.copyOf(batch.getMessages());
                    assertMessageToStore(messages.get(0), bookName, "testB", Direction.FIRST);
                    assertEquals(2, messages.get(0).getSequence());
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
        CompletableFuture<Void> store(CradleStorage storage, GroupedMessageBatchToStore batch) throws CradleStorageException, IOException;
    }
}