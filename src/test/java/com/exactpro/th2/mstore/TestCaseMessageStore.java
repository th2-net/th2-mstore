/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.mstore;

import com.exactpro.cradle.CradleObjectsFactory;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static com.exactpro.th2.common.util.StorageUtils.toInstant;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

abstract class TestCaseMessageStore<T extends GeneratedMessageV3, M extends GeneratedMessageV3> {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final int DRAIN_TIMEOUT = 1000;
    private static final long TEST_MESSAGE_BATCH_SIZE = 1024;
    private static final long TEST_EVENT_BATCH_SIZE = StoredTestEventBatch.DEFAULT_MAX_BATCH_SIZE;

    private final CradleStorage storageMock = mock(CradleStorage.class);
    @SuppressWarnings("unchecked")
    private final Persistor<StoredMessageBatch> persistorMock = mock(Persistor.class);

    @SuppressWarnings("unchecked")
    private final MessageRouter<T> routerMock = (MessageRouter<T>)mock(MessageRouter.class);

    private AbstractMessageStore<T, M> messageStore;

    private CradleObjectsFactory cradleObjectsFactory;

    @BeforeEach
    void setUp() {
        cradleObjectsFactory = spy(new CradleObjectsFactory(TEST_MESSAGE_BATCH_SIZE, TEST_EVENT_BATCH_SIZE));

        when(storageMock.getObjectsFactory()).thenReturn(cradleObjectsFactory);

        StoredMessage mockedStoredMessage = mock(StoredMessage.class);
        when(mockedStoredMessage.getTimestamp()).thenReturn(Instant.MIN);
        try {
            when(storageMock.getLastMessageIndex(any(), any())).thenReturn(-1L);
            when(storageMock.getMessage(any())).thenReturn(mockedStoredMessage);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        when(routerMock.subscribeAll(any(), any())).thenReturn(mock(SubscriberMonitor.class));
        Configuration configuration = Configuration.builder().withDrainInterval(DRAIN_TIMEOUT / 10).build();
        messageStore = spy(createStore(storageMock, routerMock, persistorMock, configuration));
        messageStore.start();
    }

    @AfterEach
    void tearDown() {
        messageStore.close();
    }

    protected abstract AbstractMessageStore<T, M> createStore(CradleStorage cradleStorageMock, MessageRouter<T> routerMock, Persistor<StoredMessageBatch> persitor, Configuration configuration);

    protected M createMessage(String session, Direction direction, long sequence) {
        return createMessage(session, direction, sequence, Instant.now());
    }

    protected abstract M createMessage(String session, Direction direction, long sequence, Instant timestamp);

    protected abstract long extractSizeInBatch(M message);

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
        void testEmptyDelivery() throws Exception {
            messageStore.handle(deliveryOf());
            verify(persistorMock, never()).persist(any());
        }

        @Test
        @DisplayName("Delivery with unordered sequences is not stored")
        void testUnorderedSequencesDelivery() throws Exception {
            String alias = "test";
            Direction direction = Direction.FIRST;
            messageStore.handle(deliveryOf(
                    createMessage(alias, direction, 2),
                    createMessage(alias, direction, 1)
            ));
            verify(persistorMock, never()).persist(any());
        }

        @Test
        @DisplayName("Delivery with equal sequences is not stored")
        void testEqualSequencesDelivery() throws Exception {
            String alias = "test";
            Direction direction = Direction.FIRST;
            messageStore.handle(deliveryOf(
                    createMessage(alias, direction, 1),
                    createMessage(alias, direction, 1)
            ));
            verify(persistorMock, never()).persist(any());
        }

        @Test
        @DisplayName("Delivery with unordered timestamps is not stored")
        void testUnorderedTimestampsDelivery() throws Exception {
            String alias = "test";
            Direction direction = Direction.FIRST;
            Instant now = Instant.now();
            messageStore.handle(deliveryOf(
                    createMessage(alias, direction, 1, now),
                    createMessage(alias, direction, 2, now.minusNanos(1)))
            );
            verify(persistorMock, never()).persist(any());
        }

        @Test
        @DisplayName("Delivery with different aliases is not stored")
        void testDifferentAliases() throws Exception {
            M first = createMessage("testA", Direction.FIRST, 1);
            M second = createMessage("testB", Direction.FIRST, 2);

            messageStore.handle(deliveryOf(first, second));
            verify(persistorMock, never()).persist(any());
        }

        @Test
        @DisplayName("Delivery with different directions is not stored")
        void testDifferentDirections() throws Exception {
            M first = createMessage("test", Direction.FIRST, 1);
            M second = createMessage("test", Direction.SECOND, 2);

            messageStore.handle(deliveryOf(first, second));
            verify(persistorMock, never()).persist(any());
        }

        @Test
        @DisplayName("Duplicated or less sequence delivery is ignored")
        void testDuplicatedOrLessSequenceDelivery() throws Exception {
            String alias = "test";
            Direction direction = Direction.FIRST;

            M first = createMessage(alias, direction, 2);
            messageStore.handle(deliveryOf(first));

            M duplicate = createMessage(alias, direction, 2);
            messageStore.handle(deliveryOf(duplicate));

            M lessSequence = createMessage(alias, direction, 1);
            messageStore.handle(deliveryOf(lessSequence));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            verify(persistorMock, timeout(DRAIN_TIMEOUT)).persist(capture.capture());
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createMessageBatch();

            StoredMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessageBatch(value, alias, direction, 1);
            assertEquals(toInstant(extractTimestamp(first)), value.getLastTimestamp());
        }

        @Test
        @DisplayName("Duplicated timestamp delivery isn't ignored but less timestamp is")
        void testDuplicatedOrLessTimestampDelivery() throws Exception {
            String alias = "test";
            Direction direction = Direction.FIRST;
            Instant now = Instant.now();

            M first = createMessage(alias, direction, 1, now);
            messageStore.handle(deliveryOf(first));

            M duplicate = createMessage(alias, direction, 2, now);
            messageStore.handle(deliveryOf(duplicate));

            M lessTimestamp = createMessage(alias, direction, 3, now.minusNanos(2));
            messageStore.handle(deliveryOf(lessTimestamp));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            verify(persistorMock, timeout(DRAIN_TIMEOUT)).persist(capture.capture());
            verify(cradleObjectsFactory, times(2 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createMessageBatch();

            StoredMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessageBatch(value, alias, direction, 2);
            assertEquals(toInstant(extractTimestamp(duplicate)), value.getLastTimestamp());
        }
    }

    @Nested
    @DisplayName("Handling single delivery")
    class TestSingleDelivery {

        @Test
        @DisplayName("Different sessions can have the same sequence")
        void testDifferentDirectionDelivery() throws Exception {
            M first = createMessage("testA", Direction.FIRST, 1);
            messageStore.handle(deliveryOf(first));

            M duplicate = createMessage("testB", Direction.SECOND, 1);
            messageStore.handle(deliveryOf(duplicate));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            verify(persistorMock, timeout(DRAIN_TIMEOUT).times(2)).persist(capture.capture());
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
        void testSingleMessageDelivery() throws Exception {
            M first = createMessage("test", Direction.FIRST, 1);

            messageStore.handle(deliveryOf(first));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            verify(persistorMock, timeout(DRAIN_TIMEOUT)).persist(capture.capture());
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createMessageBatch();

            StoredMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessageBatch(value, "test", Direction.FIRST, 1);
        }

        @Test
        @DisplayName("Delivery with ordered messages for one session are stored")
        void testNormalDelivery() throws Exception {
            M first = createMessage("test", Direction.FIRST, 1);
            M second = createMessage("test", Direction.FIRST, 2);

            messageStore.handle(deliveryOf(first, second));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            verify(persistorMock, timeout(DRAIN_TIMEOUT)).persist(capture.capture());
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createMessageBatch();

            StoredMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessageBatch(value, "test", Direction.FIRST, 2);
        }

        @Test
        @DisplayName("Delivery with messages with same timestamps for one session are stored")
        void testDeliveryWithSameTimestamps() throws Exception {
            String alias = "test";
            Direction direction = Direction.FIRST;
            Instant now = Instant.now();
            M first = createMessage(alias, direction, 1, now);
            M second = createMessage(alias, direction, 2, now);

            messageStore.handle(deliveryOf(first, second));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            verify(persistorMock, timeout(DRAIN_TIMEOUT)).persist(capture.capture());
            verify(cradleObjectsFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createMessageBatch();

            StoredMessageBatch value = capture.getValue();
            assertNotNull(value);
            assertStoredMessageBatch(value, alias, direction, 2);
            assertEquals(toInstant(extractTimestamp(second)), value.getLastTimestamp());
        }
    }

    @Nested
    @DisplayName("Several deliveries for one session")
    class TestSeveralDeliveriesInOneSession {
        @Test
        @DisplayName("Delivery for the same session ara joined to one batch")
        void joinsBatches() throws Exception {
            M first = createMessage("test", Direction.FIRST, 1);
            M second = createMessage("test", Direction.FIRST, 2);

            messageStore.handle(deliveryOf(first));
            messageStore.handle(deliveryOf(second));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            verify(persistorMock, timeout(DRAIN_TIMEOUT)).persist(capture.capture());
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
        void storesBatch() throws Exception {
            M testMsg = createMessage("test", Direction.FIRST, 1);
            if (logger.isInfoEnabled()) {
                logger.info("Test message to measure size: {}", MessageUtils.toJson(testMsg));
            }
            long oneMessageSize = extractSizeInBatch(testMsg);
            logger.info("Expected message size: {}", oneMessageSize);
            long maxMessagesInBatchCount = TEST_MESSAGE_BATCH_SIZE / oneMessageSize;
            if (TEST_EVENT_BATCH_SIZE % oneMessageSize == 0) {
                // sometimes the size of timestamp in the message is different
                // and the batch's size is multiple of message size.
                // In this case we need to decrease the total count of messages
                maxMessagesInBatchCount -= 1;
            }
            logger.info("Expected messages in one batch: {}", maxMessagesInBatchCount);
            List<M> firstDelivery = LongStream.range(0, maxMessagesInBatchCount / 2)
                    .mapToObj(it -> createMessage("test", Direction.FIRST, it))
                    .collect(Collectors.toList());

            List<M> secondDelivery = LongStream.range(maxMessagesInBatchCount / 2, maxMessagesInBatchCount + maxMessagesInBatchCount / 2)
                    .mapToObj(it -> createMessage("test", Direction.FIRST, it))
                    .collect(Collectors.toList());

            messageStore.handle(createDelivery(firstDelivery));
            messageStore.handle(createDelivery(secondDelivery));

            ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
            verify(persistorMock, timeout(DRAIN_TIMEOUT).times(2)).persist(capture.capture());
            verify(cradleObjectsFactory, times(2 + 2/*invocations in SessionBatchHolder (init + reset)*/)).createMessageBatch();

            List<StoredMessageBatch> value = capture.getAllValues();
            assertNotNull(value);
            assertEquals(2, value.size());
            assertStoredMessageBatch(value.get(0), "test", Direction.FIRST, firstDelivery.size());
            assertStoredMessageBatch(value.get(1), "test", Direction.FIRST, secondDelivery.size());
        }
    }
}