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
import com.exactpro.cradle.CradleObjectsFactory;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.messages.StoredGroupMessageBatch;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.messages.StoredMessageId;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.mstore.cfg.MessageStoreConfiguration;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Timestamp;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static com.exactpro.th2.common.util.StorageUtils.toInstant;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

abstract class TestCaseMessageStore<T extends GeneratedMessageV3, M extends GeneratedMessageV3> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final int DRAIN_TIMEOUT = 1000;
    private static final long TEST_MESSAGE_BATCH_SIZE = 1024;
    private static final long TEST_EVENT_BATCH_SIZE = StoredTestEventBatch.DEFAULT_MAX_BATCH_SIZE;

    private final CradleManager cradleManagerMock = mock(CradleManager.class);

    private final CradleStorage storageMock = mock(CradleStorage.class, RETURNS_DEEP_STUBS);

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
        when(storageMock.storeGroupedMessageBatchAsync(any(StoredGroupMessageBatch.class), any(String.class))).thenReturn(completableFuture);

        when(cradleManagerMock.getStorage()).thenReturn(storageMock);

        try {
            when(storageMock.getLastMessageIndex(any(), any())).thenReturn(-1L);
            when(storageMock.getMessage(any()).getTimestamp()).thenReturn(Instant.MIN);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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

    protected M createMessage(String session, Direction direction, long sequence) {
        return createMessage(session, "", direction, sequence, Instant.now());
    }

    protected abstract M createMessage(String session, String group, Direction direction, long sequence, Instant timestamp);

    protected abstract long extractSizeInBatch(M message);

    protected abstract T createDelivery(List<M> messages);

    protected abstract Timestamp extractTimestamp(M message);

    @NotNull
    protected MessageID createMessageId(String session, String group, Direction direction, long sequence) {

        ConnectionID.Builder connectionIdBuilder = ConnectionID.newBuilder().setSessionAlias(session);
        if (group != null)
            connectionIdBuilder.setSessionGroup(group);

        return MessageID.newBuilder()
                .setDirection(direction)
                .setSequence(sequence)
                .setConnectionId(connectionIdBuilder.build())
                .build();
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

    private static void assertStoredMessage (StoredMessage message, String streamName, Direction direction) {
        assertEquals(toCradleDirection(direction), message.getDirection());
        assertEquals(streamName, message.getStreamName());
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
        void testUnorderedSequencesDelivery() throws CradleStorageException {
            String alias = "test";
            Direction direction = Direction.FIRST;
            messageStore.handle(deliveryOf(
                    createMessage(alias, direction, 2),
                    createMessage(alias, direction, 1)
            ));
            verify(messageStore, never()).storeMessages(any(), any());
        }

        @Test
        @DisplayName("Delivery with equal sequences is not stored")
        void testEqualSequencesDelivery() throws CradleStorageException {
            String alias = "test";
            Direction direction = Direction.FIRST;
            messageStore.handle(deliveryOf(
                    createMessage(alias, direction, 1),
                    createMessage(alias, direction, 1)
            ));
            verify(messageStore, never()).storeMessages(any(), any());
        }

        @Test
        @DisplayName("Delivery with unordered timestamps is not stored")
        void testUnorderedTimestampsDelivery() throws CradleStorageException {
            String alias = "test";
            Direction direction = Direction.FIRST;
            Instant now = Instant.now();
            messageStore.handle(deliveryOf(
                    createMessage(alias, null, direction, 1, now),
                    createMessage(alias, null, direction, 2, now.minusNanos(1)))
            );
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

//        @Test
//        @DisplayName("Delivery with different directions is not stored")
//        void testDifferentDirections() throws CradleStorageException {
//            M first = createMessage("test", Direction.FIRST, 1);
//            M second = createMessage("test", Direction.SECOND, 2);
//
//            messageStore.handle(deliveryOf(first, second));
//            verify(messageStore, never()).storeMessages(any(), any());
//        }

        @Test
        @DisplayName("Duplicated or less sequence delivery is ignored")
        void testDuplicatedOrLessSequenceDelivery() {
            String alias = "test";
            Direction direction = Direction.FIRST;

            M first = createMessage(alias, direction, 2);
            messageStore.handle(deliveryOf(first));

            M duplicate = createMessage(alias, direction, 2);
            messageStore.handle(deliveryOf(duplicate));

            M lessSequence = createMessage(alias, direction, 1);
            messageStore.handle(deliveryOf(lessSequence));

            ArgumentCaptor<StoredGroupMessageBatch> capture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), capture.capture(), Mockito.eq(alias));
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createGroupMessageBatch();

            StoredGroupMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertEquals(toInstant(extractTimestamp(first)), value.getLastTimestamp());
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

            ArgumentCaptor<StoredGroupMessageBatch> batchCapture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT).times(2)), batchCapture.capture(), groupCapture.capture());
            int invocations = 2 + 2/*two sessions*/ * 2 /*invocations in SessionBatchHolder (init + reset)*/;
            verify(cradleObjectsFactory, times(invocations)).createGroupMessageBatch();

            List<StoredGroupMessageBatch> batchValues = batchCapture.getAllValues();
            assertNotNull(batchValues);
            assertEquals(2, batchValues.size());

            List<String> groupValues = groupCapture.getAllValues();
            assertNotNull(groupValues);
            assertEquals(2, groupValues.size());

            // detect list items and messages correspondence
            int id1 = -1, id2 = -1;
            for (int i = 0; i < batchValues.size(); i++) {
                StoredGroupMessageBatch batch = batchValues.get(i);
                StoredMessage msg = batch.getMessages().stream().collect(Collectors.toList()).get(0);

                if (msg.getStreamName().equals("testA"))
                    id1 = i;
                if (msg.getStreamName().equals("testB"))
                    id2 = i;
            }

            assertNotEquals(id1, -1);
            assertNotEquals(id2, -1);

            StoredGroupMessageBatch firstValue = batchValues.get(id1);
            assertStoredMessage(firstValue.getLastMessage(), "testA", Direction.FIRST);
            assertEquals(1, firstValue.getMessageCount());
            assertEquals("testA", groupValues.get(id1));

            StoredGroupMessageBatch secondValue = batchValues.get(id2);
            assertStoredMessage(secondValue.getLastMessage(), "testB", Direction.SECOND);
            assertEquals(1, secondValue.getMessageCount());
            assertEquals("testB", groupValues.get(id2));
        }

        @Test
        @DisplayName("Delivery with single message is stored normally")
        void testSingleMessageDelivery() {
            String alias = "test";
            M first = createMessage(alias, Direction.FIRST, 1);

            messageStore.handle(deliveryOf(first));

            ArgumentCaptor<StoredGroupMessageBatch> capture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), capture.capture(), Mockito.eq(alias));
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createGroupMessageBatch();

            StoredGroupMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessage(value.getLastMessage(), alias, Direction.FIRST);
            assertEquals(1, value.getMessageCount());
        }

        @Test
        @DisplayName("Delivery with session group is stored normally")
        void testSessionGroupMessageDelivery() {
            String alias = "test";
            String group = "group";
            Instant timestamp = Instant.parse("2022-03-14T12:23:23.345Z");
            M first = createMessage(alias, group, Direction.FIRST, 1, timestamp);

            messageStore.handle(deliveryOf(first));

            ArgumentCaptor<StoredGroupMessageBatch> batchCapture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), batchCapture.capture(), groupCapture.capture());
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createGroupMessageBatch();

            String groupValue = groupCapture.getValue();
            assertEquals(group, groupValue);
            StoredGroupMessageBatch batchValue = batchCapture.getValue();
            assertNotNull(batchValue);
            assertStoredMessage(batchValue.getLastMessage(), alias, Direction.FIRST);
            assertEquals(1, batchValue.getMessageCount());
        }

        @Test
        @DisplayName("Delivery with ordered messages for one session are stored")
        void testNormalDelivery() {
            String alias = "test";
            M first = createMessage(alias, Direction.FIRST, 1);
            M second = createMessage(alias, Direction.FIRST, 2);

            messageStore.handle(deliveryOf(first, second));

            ArgumentCaptor<StoredGroupMessageBatch> capture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), capture.capture(), Mockito.eq(alias));
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createGroupMessageBatch();

            StoredGroupMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessage(value.getLastMessage(), alias, Direction.FIRST);
            assertEquals(2, value.getMessageCount());
        }

        @Test
        @DisplayName("Delivery with messages with same timestamps for one session are stored")
        void testDeliveryWithSameTimestamps() {
            String alias = "test";
            Direction direction = Direction.FIRST;
            Instant now = Instant.now();
            M first = createMessage(alias, null, direction, 1, now);
            M second = createMessage(alias, null, direction, 2, now);

            messageStore.handle(deliveryOf(first, second));

            ArgumentCaptor<StoredGroupMessageBatch> capture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), capture.capture(), Mockito.eq(alias));
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createGroupMessageBatch();

            StoredGroupMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessage(value.getLastMessage(), alias, Direction.FIRST);
            assertEquals(2, value.getMessageCount());
            assertEquals(toInstant(extractTimestamp(second)), value.getLastTimestamp());
        }
    }

    @Nested
    @DisplayName("Several deliveries for one session")
    class TestSeveralDeliveriesInOneSession {
        @Test
        @DisplayName("Delivery for the same session ara joined to one batch")
        void joinsBatches() {
            String alias = "test";
            M first = createMessage(alias, Direction.FIRST, 1);
            M second = createMessage(alias, Direction.FIRST, 2);

            messageStore.handle(deliveryOf(first));
            messageStore.handle(deliveryOf(second));

            ArgumentCaptor<StoredGroupMessageBatch> capture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT)), capture.capture(), Mockito.eq(alias));
            verify(cradleObjectsFactory, times(2 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createGroupMessageBatch();

            StoredGroupMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessage(value.getLastMessage(), alias, Direction.FIRST);
            assertEquals(2, value.getMessageCount());
        }

        /**
         * Message size calculation changed in Cradle, please see
         * {@link com.exactpro.cradle.serialization.MessagesSizeCalculator#calculateMessageSizeInBatch(com.exactpro.cradle.messages.MessageToStore)}
         * {@link com.exactpro.cradle.serialization.MessagesSizeCalculator#calculateMessageSize(com.exactpro.cradle.messages.MessageToStore)}
         */
        @Test
        @DisplayName("Stores batch if cannot join because of messages size")
        void storesBatch() {
            String alias = "test";
            M testMsg = createMessage(alias, Direction.FIRST, 1);
            if (logger.isInfoEnabled()) {
                logger.info("Test message to measure size: {}", MessageUtils.toJson(testMsg));
            }
            long oneMessageSize = extractSizeInBatch(testMsg);
            logger.info("Expected message size: {}", oneMessageSize);
            long maxMessagesInBatchCount = TEST_MESSAGE_BATCH_SIZE / oneMessageSize - 1;
            if (TEST_EVENT_BATCH_SIZE % oneMessageSize == 0) {
                // sometimes the size of timestamp in the message is different
                // and the batch's size is multiple of message size.
                // In this case we need to decrease the total count of messages
                maxMessagesInBatchCount -= 1;
            }
            logger.info("Expected messages in one batch: {}", maxMessagesInBatchCount);
            List<M> firstDelivery = LongStream.range(0, maxMessagesInBatchCount / 2)
                    .mapToObj(it -> createMessage(alias, Direction.FIRST, it))
                    .collect(Collectors.toList());

            List<M> secondDelivery = LongStream.range(maxMessagesInBatchCount / 2, maxMessagesInBatchCount + maxMessagesInBatchCount / 2)
                    .mapToObj(it -> createMessage(alias, Direction.FIRST, it))
                    .collect(Collectors.toList());

            messageStore.handle(createDelivery(firstDelivery));
            messageStore.handle(createDelivery(secondDelivery));

            ArgumentCaptor<StoredGroupMessageBatch> capture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT).times(2)), capture.capture(), Mockito.eq(alias));
            verify(cradleObjectsFactory, times(2 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createGroupMessageBatch();

            List<StoredGroupMessageBatch> value = capture.getAllValues();
            assertNotNull(value);
            assertEquals(2, value.size());

            assertStoredMessage(value.get(0).getLastMessage(), alias, Direction.FIRST);
            assertEquals(firstDelivery.size(), value.get(0).getMessageCount());

            assertStoredMessage(value.get(1).getLastMessage(), alias, Direction.FIRST);
            assertEquals(secondDelivery.size(), value.get(1).getMessageCount());
        }
    }

    @Test
    @DisplayName("Close message store when feature is complited")
    void testCompletedFutureCompleted() throws InterruptedException, ExecutionException, TimeoutException {
        M first = createMessage("test", Direction.FIRST, 1);

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

        M first = createMessage("test", Direction.FIRST, 1);

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

        M first = createMessage("test", Direction.FIRST, 1);

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

        M first = createMessage("test", Direction.FIRST, 1);

        messageStore.handle(deliveryOf(first));
        messageStore.dispose();

        verify(completableFuture).get(any(long.class), any(TimeUnit.class));
        verify(completableFuture).isDone();
    }

    protected interface CradleStoreFunction {
        CompletableFuture<Void> store(CradleStorage storage, StoredGroupMessageBatch batch, String sessionGroup);
    }
}