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
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
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

    private final CradleStorage storageMock = mock(CradleStorage.class);

    @SuppressWarnings("unchecked")
    private final Persistor persistor = mock(Persistor.class);

    @SuppressWarnings("unchecked")
    private final MessageRouter<T> routerMock = (MessageRouter<T>)mock(MessageRouter.class);

    @SuppressWarnings("unchecked")
    private final CompletableFuture<Void> completableFuture = mock(CompletableFuture.class);

    private AbstractMessageStore<T, M> messageStore;

    private CradleObjectsFactory cradleObjectsFactory;

    @BeforeEach
    void setUp() {
        cradleObjectsFactory = spy(new CradleObjectsFactory(TEST_MESSAGE_BATCH_SIZE, TEST_EVENT_BATCH_SIZE));

        when(storageMock.getObjectsFactory()).thenReturn(cradleObjectsFactory);
        when(cradleManagerMock.getStorage()).thenReturn(storageMock);

        StoredMessage mockedStoredMessage = mock(StoredMessage.class);
        when(mockedStoredMessage.getTimestamp()).thenReturn(Instant.MIN);
        try {
            when(storageMock.getLastMessageIndex(any(), any())).thenReturn(-1L);
            when(storageMock.getMessage(any())).thenReturn(mockedStoredMessage);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        when(routerMock.subscribeAll(any(), any())).thenReturn(mock(SubscriberMonitor.class));
        Configuration configuration = Configuration.builder()
                                                   .withDrainInterval(DRAIN_TIMEOUT / 10)
                                                   .build();
        messageStore = spy(createStore(storageMock, routerMock, persistor, configuration));
        messageStore.start();
    }

    @AfterEach
    void tearDown() {
        messageStore.close();
    }

    protected abstract AbstractMessageStore<T, M> createStore(CradleStorage cradleStorageMock,
                                                              MessageRouter<T> routerMock,
                                                              Persistor<StoredGroupMessageBatch> persistor,
                                                              Configuration configuration);

    protected M createMessage(String session, String group, Direction direction, long sequence) {
        return createMessage(session, group, direction, sequence, Instant.now());
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

    private static void assertStoredMessage (StoredMessage message, String streamName, Direction direction) {
        assertEquals(toCradleDirection(direction), message.getDirection());
        assertEquals(streamName, message.getStreamName());
    }

    @Nested
    @DisplayName("Incorrect delivery content")
    class TestNegativeCases {

        @Test
        @DisplayName("Empty delivery is not stored")
        void testEmptyDelivery() throws Exception {
            messageStore.handle(deliveryOf());
            verify(persistor, never()).persist(any());
        }

        @Test
        @DisplayName("Delivery with unordered sequences is not stored")
        void testUnorderedSequencesDelivery() throws Exception {
            String alias = "test-session";
            String group = "test-group";
            Direction direction = Direction.FIRST;
            messageStore.handle(deliveryOf(
                    createMessage(alias, group, direction, 2),
                    createMessage(alias, group, direction, 1)
            ));
            verify(persistor, never()).persist(any());
        }

        @Test
        @DisplayName("Delivery with equal sequences is not stored")
        void testEqualSequencesDelivery() throws Exception {
            String alias = "test-session";
            String group = "test-group";
            Direction direction = Direction.FIRST;
            messageStore.handle(deliveryOf(
                    createMessage(alias, group, direction, 1),
                    createMessage(alias, group, direction, 1)
            ));
            verify(persistor, never()).persist(any());
        }

        @Test
        @DisplayName("Delivery with unordered timestamps is not stored")
        void testUnorderedTimestampsDelivery() throws Exception {
            String alias = "test-session";
            String group = "test-group";
            Direction direction = Direction.FIRST;
            Instant now = Instant.now();
            messageStore.handle(deliveryOf(
                    createMessage(alias, group, direction, 1, now),
                    createMessage(alias, group, direction, 2, now.minusNanos(1)))
            );
            verify(persistor, never()).persist(any());
        }

        @Test
        @DisplayName("Delivery with different aliases is not stored")
        void testDifferentAliases() throws Exception {
            String group = "test-group";
            M first = createMessage("testA", group, Direction.FIRST, 1);
            M second = createMessage("testB", group, Direction.FIRST, 2);

            messageStore.handle(deliveryOf(first, second));
            verify(persistor, never()).persist(any());
        }


        @Test
        @DisplayName("Duplicated or less sequence delivery is ignored")
        void testDuplicatedOrLessSequenceDelivery() throws Exception {
            String alias = "test-session";
            String group = "test-group";
            Direction direction = Direction.FIRST;

            M first = createMessage(alias, group, direction, 2);
            messageStore.handle(deliveryOf(first));

            M duplicate = createMessage(alias, group, direction, 2);
            messageStore.handle(deliveryOf(duplicate));

            M lessSequence = createMessage(alias, group, direction, 1);
            messageStore.handle(deliveryOf(lessSequence));

            ArgumentCaptor<StoredGroupMessageBatch> capture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            verify(persistor, timeout(DRAIN_TIMEOUT)).persist(capture.capture());
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/))
                    .createGroupMessageBatch(Mockito.eq(group));

            StoredGroupMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertEquals(toInstant(extractTimestamp(first)), value.getLastTimestamp());
            assertEquals(group, value.getSessionGroup());
        }
    }

    @Nested
    @DisplayName("Handling single delivery")
    class TestSingleDelivery {

        @Test
        @DisplayName("Different sessions can have the same sequence")
        void testDifferentDirectionDelivery()  throws Exception {
            M first = createMessage("testA", "groupA", Direction.FIRST, 1);
            messageStore.handle(deliveryOf(first));

            M duplicate = createMessage("testB", "groupB", Direction.SECOND, 1);
            messageStore.handle(deliveryOf(duplicate));

            ArgumentCaptor<StoredGroupMessageBatch> batchCapture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            verify(persistor, timeout(DRAIN_TIMEOUT).times(2)).persist(batchCapture.capture());
            int invocations = 2 + 2/*two sessions*/ * 2 /*invocations in SessionBatchHolder (init + reset)*/;
            verify(cradleObjectsFactory, times(invocations)).createGroupMessageBatch(any());

            List<StoredGroupMessageBatch> batchValues = batchCapture.getAllValues();
            assertNotNull(batchValues);
            assertEquals(2, batchValues.size());

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
            assertEquals("groupA", firstValue.getSessionGroup());

            StoredGroupMessageBatch secondValue = batchValues.get(id2);
            assertStoredMessage(secondValue.getLastMessage(), "testB", Direction.SECOND);
            assertEquals(1, secondValue.getMessageCount());
            assertEquals("groupB", secondValue.getSessionGroup());
        }

        @Test
        @DisplayName("Delivery with single message is stored normally")
        void testSingleMessageDelivery() throws Exception {
            String alias = "test-session";
            String group = "test-group";
            M first = createMessage(alias, group, Direction.FIRST, 1);

            messageStore.handle(deliveryOf(first));

            ArgumentCaptor<StoredGroupMessageBatch> capture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            verify(persistor, timeout(DRAIN_TIMEOUT)).persist(capture.capture());
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/))
                    .createGroupMessageBatch(Mockito.eq(group));

            StoredGroupMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessage(value.getLastMessage(), alias, Direction.FIRST);
            assertEquals(1, value.getMessageCount());
            assertEquals(group, value.getSessionGroup());
        }

        @Test
        @DisplayName("Delivery with session group is stored normally")
        void testSessionGroupMessageDelivery() throws Exception {
            String alias = "test";
            String group = "group";
            Instant timestamp = Instant.parse("2022-03-14T12:23:23.345Z");
            M first = createMessage(alias, group, Direction.FIRST, 1, timestamp);

            messageStore.handle(deliveryOf(first));

            ArgumentCaptor<StoredGroupMessageBatch> batchCapture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            verify(persistor, timeout(DRAIN_TIMEOUT)).persist(batchCapture.capture());
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/))
                    .createGroupMessageBatch(Mockito.eq(group));

            StoredGroupMessageBatch batchValue = batchCapture.getValue();
            assertNotNull(batchValue);
            assertEquals(group, batchValue.getSessionGroup());
            assertStoredMessage(batchValue.getLastMessage(), alias, Direction.FIRST);
            assertEquals(1, batchValue.getMessageCount());
        }

        @Test
        @DisplayName("Delivery with ordered messages for one session are stored")
        void testNormalDelivery() throws Exception {
            String alias = "test-session";
            String group = "test-group";
            M first = createMessage(alias, group, Direction.FIRST, 1);
            M second = createMessage(alias, group, Direction.FIRST, 2);

            messageStore.handle(deliveryOf(first, second));

            ArgumentCaptor<StoredGroupMessageBatch> capture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            verify(persistor, timeout(DRAIN_TIMEOUT)).persist(capture.capture());
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/))
                    .createGroupMessageBatch(Mockito.eq(group));

            StoredGroupMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessage(value.getLastMessage(), alias, Direction.FIRST);
            assertEquals(2, value.getMessageCount());
            assertEquals(group, value.getSessionGroup());
        }

        @Test
        @DisplayName("Delivery with messages with same timestamps for one session are stored")
        void testDeliveryWithSameTimestamps() throws Exception {
            String alias = "test-session";
            String group = "test-group";
            Direction direction = Direction.FIRST;
            Instant now = Instant.now();
            M first = createMessage(alias, group, direction, 1, now);
            M second = createMessage(alias, group, direction, 2, now);

            messageStore.handle(deliveryOf(first, second));

            ArgumentCaptor<StoredGroupMessageBatch> capture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            verify(persistor, timeout(DRAIN_TIMEOUT)).persist(capture.capture());
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/))
                    .createGroupMessageBatch(Mockito.eq(group));

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
        void joinsBatches() throws Exception {
            String alias = "test-session";
            String group = "test-group";
            M first = createMessage(alias, group, Direction.FIRST, 1);
            M second = createMessage(alias, group, Direction.FIRST, 2);

            messageStore.handle(deliveryOf(first));
            messageStore.handle(deliveryOf(second));

            ArgumentCaptor<StoredGroupMessageBatch> capture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            verify(persistor, timeout(DRAIN_TIMEOUT)).persist(capture.capture());
            verify(cradleObjectsFactory, times(2 + 2/*invocations in SessionBatchHolder (init + reset)*/))
                    .createGroupMessageBatch(Mockito.eq(group));

            StoredGroupMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessage(value.getLastMessage(), alias, Direction.FIRST);
            assertEquals(2, value.getMessageCount());
            assertEquals(group, value.getSessionGroup());
        }

        /**
         * Message size calculation changed in Cradle, please see
         * {@link com.exactpro.cradle.serialization.MessagesSizeCalculator#calculateMessageSizeInBatch(com.exactpro.cradle.messages.MessageToStore)}
         * {@link com.exactpro.cradle.serialization.MessagesSizeCalculator#calculateMessageSize(com.exactpro.cradle.messages.MessageToStore)}
         */
        @Test
        @DisplayName("Stores batch if cannot join because of messages size")
        void storesBatch() throws Exception {
            String alias = "test-session";
            String group = "test-group";
            M testMsg = createMessage(alias, group, Direction.FIRST, 1);
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
                    .mapToObj(it -> createMessage(alias, group, Direction.FIRST, it))
                    .collect(Collectors.toList());

            List<M> secondDelivery = LongStream.range(maxMessagesInBatchCount / 2, maxMessagesInBatchCount + maxMessagesInBatchCount / 2)
                    .mapToObj(it -> createMessage(alias, group, Direction.FIRST, it))
                    .collect(Collectors.toList());

            messageStore.handle(createDelivery(firstDelivery));
            messageStore.handle(createDelivery(secondDelivery));

            ArgumentCaptor<StoredGroupMessageBatch> capture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
            verify(persistor, timeout(DRAIN_TIMEOUT).times(2)).persist(capture.capture());
            verify(cradleObjectsFactory, times(2 + 2/*invocations in SessionBatchHolder (init + reset)*/))
                    .createGroupMessageBatch(Mockito.eq(group));

            List<StoredGroupMessageBatch> value = capture.getAllValues();
            assertNotNull(value);
            assertEquals(2, value.size());

            assertStoredMessage(value.get(0).getLastMessage(), alias, Direction.FIRST);
            assertEquals(firstDelivery.size(), value.get(0).getMessageCount());

            assertStoredMessage(value.get(1).getLastMessage(), alias, Direction.FIRST);
            assertEquals(secondDelivery.size(), value.get(1).getMessageCount());
        }
    }
}