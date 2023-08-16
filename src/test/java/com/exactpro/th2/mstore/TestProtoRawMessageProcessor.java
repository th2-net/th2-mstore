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

import com.exactpro.cradle.CoreStorageSettings;
import com.exactpro.cradle.CradleEntitiesFactory;
import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.messages.CradleMessage;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredGroupedMessageBatch;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.grpc.RawMessageOrBuilder;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.util.StorageUtils;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampOrBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Random;

import static com.exactpro.th2.common.event.EventUtils.toTimestamp;
import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static com.exactpro.th2.mstore.ProtoRawMessageProcessor.toCradleMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestProtoRawMessageProcessor {
    private static final int DRAIN_TIMEOUT = 1000;
    private static final int TEST_MESSAGE_BATCH_SIZE = 1024;
    private static final int TEST_EVENT_BATCH_SIZE = CradleStorage.DEFAULT_MAX_MESSAGE_BATCH_SIZE;

    private final CradleManager cradleManagerMock = mock(CradleManager.class);
    private final CradleStorage storageMock = mock(CradleStorage.class);
    @SuppressWarnings("unchecked")
    private final MessageRouter<RawMessageBatch> routerMock = (MessageRouter<RawMessageBatch>) mock(MessageRouter.class);

    @SuppressWarnings("SpellCheckingInspection")
    private final MessagePersistor persistor = mock(MessagePersistor.class);
    private final DeliveryMetadata deliveryMetadata = new DeliveryMetadata("", false);
    private final ManualAckDeliveryCallback.Confirmation confirmation = mock(ManualAckDeliveryCallback.Confirmation.class);

    private final Random random = new Random();

    private ProtoRawMessageProcessor messageProcessor;

    private CradleEntitiesFactory cradleEntitiesFactory;


    @BeforeEach
    void setUp() throws CradleStorageException, IOException {
        cradleEntitiesFactory = spy(new CradleEntitiesFactory(
                TEST_MESSAGE_BATCH_SIZE,
                TEST_EVENT_BATCH_SIZE,
                new CoreStorageSettings().calculateStoreActionRejectionThreshold()
        ));

        //noinspection unchecked
        CassandraCradleResultSet<StoredMessage> rsMock = (CassandraCradleResultSet<StoredMessage>) mock(CassandraCradleResultSet.class);
        when(rsMock.next()).thenReturn(null);
        when(storageMock.getMessages(any())).thenReturn(rsMock);

        when(storageMock.getEntitiesFactory()).thenReturn(cradleEntitiesFactory);
        when(cradleManagerMock.getStorage()).thenReturn(storageMock);
        when(routerMock.subscribeAllWithManualAck(any(), any(), any())).thenReturn(mock(SubscriberMonitor.class));
        Configuration configuration = Configuration.builder().withDrainInterval(DRAIN_TIMEOUT / 10).build();
        messageProcessor = spy(createStore(storageMock, routerMock, persistor, configuration));
        messageProcessor.start();
    }

    @Test
    public void testToCradleMessage() throws CradleStorageException {
        RawMessage rawMessage = RawMessage.newBuilder()
                .setMetadata(RawMessageMetadata.newBuilder()
                        .setProtocol("test-protocol")
                        .putProperties("test-key", "test-value")
                        .setId(MessageID.newBuilder()
                                .setConnectionId(ConnectionID.newBuilder()
//                                        .setSessionGroup("test-session-group") // this paremter isn't present in MessageToStore
                                        .setSessionAlias("test-session-alias")
                                        .build())
                                .setBookName("test-book")
                                .setTimestamp(MessageUtils.toTimestamp(Instant.now()))
                                .setDirection(Direction.SECOND)
                                .setSequence(6234)
                                .build())
                        .build())
                .setBody(ByteString.copyFrom(new byte[]{1, 4, 2, 3}))
                .build();

        MessageToStore cradleMessage = toCradleMessage(rawMessage);

        Assertions.assertEquals(rawMessage.getMetadata().getProtocol(), cradleMessage.getProtocol());
        Assertions.assertEquals(rawMessage.getMetadata().getPropertiesMap(), cradleMessage.getMetadata().toMap());
        Assertions.assertEquals(rawMessage.getMetadata().getId().getConnectionId().getSessionAlias(), cradleMessage.getSessionAlias());
        Assertions.assertEquals(rawMessage.getMetadata().getId().getBookName(), cradleMessage.getBookId().getName());
        Assertions.assertEquals(StorageUtils.toCradleDirection(rawMessage.getMetadata().getId().getDirection()), cradleMessage.getDirection());
        Assertions.assertEquals(StorageUtils.toInstant(rawMessage.getMetadata().getId().getTimestamp()), cradleMessage.getTimestamp());
        Assertions.assertEquals(rawMessage.getMetadata().getId().getSequence(), cradleMessage.getSequence());
        Assertions.assertArrayEquals(rawMessage.getBody().toByteArray(), cradleMessage.getContent());
    }

    @AfterEach
    void tearDown() {
        messageProcessor.close();
    }


    @NotNull
    protected static MessageID createMessageId(Instant timestamp, String sessionAlias, String sessionGroup, Direction direction, long sequence, String bookName) {
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

    private static String bookName(int index) {
        return "book-name-" + index;
    }

    private RawMessageBatch deliveryOf(RawMessage... messages) {
        return createDelivery(List.of(messages));
    }

    private static void assertMessageToStore(CradleMessage message, String bookName, String sessionAlias, Direction direction) {
        assertEquals(bookName, message.getBookId().getName());
        assertEquals(sessionAlias, message.getSessionAlias());
        assertEquals(toCradleDirection(direction), message.getDirection());
    }

    private static void assertMessageBatchToStore(StoredGroupedMessageBatch batch, String bookName, String groupName, int count) {
        assertEquals(bookName, batch.getBookId().getName());
        assertEquals(groupName, batch.getGroup());
        assertEquals(count, batch.getMessageCount());
    }

    private static void assertAllGroupValuesMatchTo(ArgumentCaptor<String> capture, String value, int count) {
        List<String> values = capture.getAllValues();
        if (count == 0) {
            if (!(values == null || values.isEmpty())) {
                Assertions.fail("Expecting empty values for groups");
            }
            return;
        }

        assertNotNull(values);
        assertEquals(count, values.size());
        values.forEach(group -> {
            if (value == null) {
                assertNull(group);
            } else {
                assertEquals(value, group);
            }
        });
    }

    @Nested
    @DisplayName("Incorrect delivery content")
    class TestNegativeCases {

        @Test
        @DisplayName("Empty delivery is not stored")
        void testEmptyDelivery() {
            messageProcessor.process(deliveryMetadata, deliveryOf(), confirmation);
            verify(persistor, never()).persist(any(), any());
        }

        @Test
        @DisplayName("Delivery with unordered sequences is not stored")
        void testUnorderedDelivery() {
            String bookName = bookName(random.nextInt());
            RawMessage first = createMessage("test", "group", Direction.FIRST, 1, bookName);
            RawMessage second = createMessage("test", "group", Direction.FIRST, 2, bookName);

            messageProcessor.process(deliveryMetadata, deliveryOf(second, first), confirmation);
            verify(persistor, never()).persist(any(), any());
        }

        @Test
        @DisplayName("Duplicated delivery is ignored")
        void testDuplicatedDelivery() {
            String bookName = bookName(random.nextInt());

            RawMessage first = createMessage("test", "group", Direction.FIRST, 1, bookName);
            messageProcessor.process(deliveryMetadata, deliveryOf(first), confirmation);

            RawMessage duplicate = createMessage("test", "group", Direction.FIRST, 1, bookName);
            messageProcessor.process(deliveryMetadata, deliveryOf(duplicate), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            verify(persistor, timeout(DRAIN_TIMEOUT)).persist(batchCapture.capture(), any());
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
        void testDifferentDirectionDelivery() {
            String bookName = bookName(random.nextInt());

            RawMessage first = createMessage("testA", "group", Direction.FIRST, 1, bookName);
            messageProcessor.process(deliveryMetadata, deliveryOf(first), confirmation);

            RawMessage duplicate = createMessage("testB", "group", Direction.SECOND, 1, bookName);
            messageProcessor.process(deliveryMetadata, deliveryOf(duplicate), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            verify(persistor, timeout(DRAIN_TIMEOUT).times(1)).persist(batchCapture.capture(), any());
            int invocations = 2 + 2 /*invocations in SessionBatchHolder (init + reset)*/;
            verify(cradleEntitiesFactory, times(invocations)).groupedMessageBatch(groupCapture.capture());

            List<GroupedMessageBatchToStore> value = batchCapture.getAllValues();
            assertNotNull(value);
            assertEquals(1, value.size());

            GroupedMessageBatchToStore batch = value.get(0);

            List<StoredMessage> messages = List.copyOf(batch.getMessages());
            assertEquals(2, messages.size());
            boolean firstCase = false;
            boolean secondCase = false;
            for (StoredMessage message : messages) {
                if (message.getDirection() == toCradleDirection(Direction.FIRST)) {
                    assertMessageToStore(message, bookName, "testA", Direction.FIRST);
                    firstCase = true;
                } else {
                    assertMessageToStore(message, bookName, "testB", Direction.SECOND);
                    secondCase = true;
                }
            }
            assertTrue(firstCase);
            assertTrue(secondCase);

            assertAllGroupValuesMatchTo(groupCapture, "group", 4);
        }

        @Test
        @DisplayName("Delivery with single message is stored normally")
        void testSingleMessageDelivery() {
            String bookName = bookName(random.nextInt());
            RawMessage first = createMessage("test", "group", Direction.FIRST, 1, bookName);

            messageProcessor.process(deliveryMetadata, deliveryOf(first), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            verify(persistor, timeout(DRAIN_TIMEOUT)).persist(batchCapture.capture(), any());
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
        void testNormalDelivery() {
            String bookName = bookName(random.nextInt());
            RawMessage first = createMessage("test", "group", Direction.FIRST, 1, bookName);
            RawMessage second = createMessage("test", "group", Direction.FIRST, 2, bookName);

            messageProcessor.process(deliveryMetadata, deliveryOf(first, second), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            verify(persistor, timeout(DRAIN_TIMEOUT)).persist(batchCapture.capture(), any());
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
        void joinsBatches() {
            String bookName = bookName(random.nextInt());
            RawMessage first = createMessage("test", "group", Direction.FIRST, 1, bookName);
            RawMessage second = createMessage("test", "group", Direction.FIRST, 2, bookName);

            messageProcessor.process(deliveryMetadata, deliveryOf(first), confirmation);
            messageProcessor.process(deliveryMetadata, deliveryOf(second), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            verify(persistor, timeout(DRAIN_TIMEOUT)).persist(batchCapture.capture(), any());
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

        @Test
        @DisplayName("Several deliveries have one session, increasing sequences, but decreasing timestamps")
        void rejectsDecreasingTimestamps() throws Exception {
            String bookName = bookName(random.nextInt());
            RawMessage secondToProcess = createMessage("test", "group", Direction.FIRST, 2, bookName);
            Thread.sleep(1);
            RawMessage firstToProcess = createMessage("test", "group", Direction.FIRST, 1, bookName);

            messageProcessor.process(deliveryMetadata, deliveryOf(firstToProcess), confirmation);
            messageProcessor.process(deliveryMetadata, deliveryOf(secondToProcess), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);

            verify(persistor, timeout(DRAIN_TIMEOUT).times(1)).persist(batchCapture.capture(), any());
            verify(confirmation, timeout(DRAIN_TIMEOUT).times(1)).reject();

        }

        @Test
        @DisplayName("batch with older message is not stored. case 1")
        void rejectsDecreasingGroup1() throws Exception {

            Instant last = Instant.parse("2023-01-01T00:00:00Z");

            doReturn(last).when(messageProcessor).loadLastMessageTimestamp(any(), any());


            String bookName = bookName(random.nextInt());

            RawMessage b1m1 = createMessage("test1", "group", Direction.FIRST, 1, last.plusMillis(10), bookName);
            RawMessage b1m2 = createMessage("test2", "group", Direction.FIRST, 2, last.minusMillis(20), bookName);

            messageProcessor.process(deliveryMetadata, deliveryOf(b1m1, b1m2), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);

            verify(persistor, timeout(DRAIN_TIMEOUT).times(0)).persist(batchCapture.capture(), any());
            verify(confirmation, timeout(DRAIN_TIMEOUT).times(1)).reject();
        }

        @Test
        @DisplayName("batch with older message is not stored. case 2")
        void rejectsDecreasingGroup2() throws Exception {

            Instant last = Instant.parse("2023-01-01T00:00:00Z");

            doReturn(last).when(messageProcessor).loadLastMessageTimestamp(any(), any());


            String bookName = bookName(random.nextInt());

            RawMessage b1m1 = createMessage("test1", "group", Direction.FIRST, 1, last.plusMillis(10), bookName);
            RawMessage b1m2 = createMessage("test2", "group", Direction.FIRST, 2, last.plusMillis(20), bookName);

            RawMessage b2m1 = createMessage("test1", "group", Direction.FIRST, 1, last.plusMillis(30), bookName);
            RawMessage b2m2 = createMessage("test4", "group", Direction.FIRST, 2, last.plusMillis(10), bookName);

            messageProcessor.process(deliveryMetadata, deliveryOf(b1m1, b1m2), confirmation);
            messageProcessor.process(deliveryMetadata, deliveryOf(b2m1, b2m2), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);

            verify(persistor, timeout(DRAIN_TIMEOUT).times(1)).persist(batchCapture.capture(), any());
            verify(confirmation, timeout(DRAIN_TIMEOUT).times(1)).reject();
        }
    }

    @Test
    @DisplayName("Delivery with different aliases is stored")
    void testDifferentAliases() {
        String bookName = bookName(random.nextInt());
        RawMessage first = createMessage("testA", "group", Direction.FIRST, 1, bookName);
        RawMessage second = createMessage("testB", "group", Direction.FIRST, 2, bookName);

        messageProcessor.process(deliveryMetadata, deliveryOf(first, second), confirmation);
        verify(persistor, timeout(DRAIN_TIMEOUT).times(1)).persist(any(), any());
    }

    @Test
    @DisplayName("Delivery with different directions is stored")
    void testDifferentDirections() {
        String bookName = bookName(random.nextInt());
        RawMessage first = createMessage("test", "group", Direction.FIRST, 1, bookName);
        RawMessage second = createMessage("test", "group", Direction.SECOND, 2, bookName);

        messageProcessor.process(deliveryMetadata, deliveryOf(first, second), confirmation);
        verify(persistor, timeout(DRAIN_TIMEOUT).times(1)).persist(any(), any());
    }


    @Nested
    @DisplayName("Deliveries for different groups")
    class TestDeliveriesForDifferentGroups {
        @Test
        @DisplayName("Deliveries for different groups are stored separately")
        void separateBatches() {
            String bookName = bookName(random.nextInt());
            RawMessage first = createMessage("testA", "group1", Direction.FIRST, 1, bookName);
            RawMessage second = createMessage("testB", "group2", Direction.FIRST, 2, bookName);

            messageProcessor.process(deliveryMetadata, deliveryOf(first), confirmation);
            messageProcessor.process(deliveryMetadata, deliveryOf(second), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            verify(persistor, timeout(DRAIN_TIMEOUT).times(2)).persist(batchCapture.capture(), any());
            verify(cradleEntitiesFactory, times(2 + 4/*invocations in SessionBatchHolder (init + reset)*/))
                    .groupedMessageBatch(groupCapture.capture());

            List<GroupedMessageBatchToStore> batchValues = batchCapture.getAllValues();
            assertNotNull(batchValues);
            assertEquals(2, batchValues.size());

            List<String> groupValues = groupCapture.getAllValues();
            assertNotNull(groupValues);
            assertEquals(6, groupValues.size());

            for (GroupedMessageBatchToStore batch : batchValues) {
                String group = batch.getGroup();
                if ("group1".equals(group)) {
                    List<StoredMessage> messages = List.copyOf(batch.getMessages());
                    assertMessageToStore(messages.get(0), bookName, "testA", Direction.FIRST);
                    assertEquals(1, messages.get(0).getSequence());
                }
                if ("group2".equals(group)) {
                    List<StoredMessage> messages = List.copyOf(batch.getMessages());
                    assertMessageToStore(messages.get(0), bookName, "testB", Direction.FIRST);
                    assertEquals(2, messages.get(0).getSequence());
                }
            }
        }
    }

    @Nested
    @DisplayName("Deliveries for different books")
    class TestDeliveriesForDifferentBooks {

        @Test
        @DisplayName("Deliveries for different books and the same group are stored separately")
        void separateBatchesWithTheSameGroup() {
            String bookName1 = "bookA";
            String bookName2 = "bookB";
            String sessionGroup = "group";

            RawMessage first = createMessage("testA", sessionGroup, Direction.FIRST, 1, bookName1);
            RawMessage second = createMessage("testB", sessionGroup, Direction.FIRST, 2, bookName2);

            messageProcessor.process(deliveryMetadata, deliveryOf(first), confirmation);
            messageProcessor.process(deliveryMetadata, deliveryOf(second), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            verify(persistor, timeout(DRAIN_TIMEOUT).times(2)).persist(batchCapture.capture(), any());
            verify(cradleEntitiesFactory, times(2 + 4/*invocations in SessionBatchHolder (init + reset)*/))
                    .groupedMessageBatch(groupCapture.capture());

            List<GroupedMessageBatchToStore> batchValues = batchCapture.getAllValues();
            assertNotNull(batchValues);
            assertEquals(2, batchValues.size());

            List<String> groupValues = groupCapture.getAllValues();
            assertNotNull(groupValues);
            assertEquals(6, groupValues.size());

            for (GroupedMessageBatchToStore batch : batchValues) {
                assertEquals(sessionGroup, batch.getGroup());
                String bookNmae = batch.getBookId().getName();
                if (bookNmae.equals(bookName1)) {
                    List<StoredMessage> messages = List.copyOf(batch.getMessages());
                    assertMessageToStore(messages.get(0), bookName1, "testA", Direction.FIRST);
                    assertEquals(1, messages.get(0).getSequence());
                }
                if (bookNmae.equals(bookName2)) {
                    List<StoredMessage> messages = List.copyOf(batch.getMessages());
                    assertMessageToStore(messages.get(0), bookName2, "testB", Direction.FIRST);
                    assertEquals(2, messages.get(0).getSequence());
                }
            }
        }

        @Test
        @DisplayName("Deliveries for different groups are stored separately")
        void separateBatches() {
            String bookName = bookName(random.nextInt());
            RawMessage first = createMessage("testA", "group1", Direction.FIRST, 1, bookName);
            RawMessage second = createMessage("testB", "group2", Direction.FIRST, 2, bookName);

            messageProcessor.process(deliveryMetadata, deliveryOf(first), confirmation);
            messageProcessor.process(deliveryMetadata, deliveryOf(second), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            verify(persistor, timeout(DRAIN_TIMEOUT).times(2)).persist(batchCapture.capture(), any());
            verify(cradleEntitiesFactory, times(2 + 4/*invocations in SessionBatchHolder (init + reset)*/))
                    .groupedMessageBatch(groupCapture.capture());

            List<GroupedMessageBatchToStore> batchValues = batchCapture.getAllValues();
            assertNotNull(batchValues);
            assertEquals(2, batchValues.size());

            List<String> groupValues = groupCapture.getAllValues();
            assertNotNull(groupValues);
            assertEquals(6, groupValues.size());

            for (GroupedMessageBatchToStore batch : batchValues) {
                String group = batch.getGroup();
                if ("group1".equals(group)) {
                    List<StoredMessage> messages = List.copyOf(batch.getMessages());
                    assertMessageToStore(messages.get(0), bookName, "testA", Direction.FIRST);
                    assertEquals(1, messages.get(0).getSequence());
                }
                if ("group2".equals(group)) {
                    List<StoredMessage> messages = List.copyOf(batch.getMessages());
                    assertMessageToStore(messages.get(0), bookName, "testB", Direction.FIRST);
                    assertEquals(2, messages.get(0).getSequence());
                }
            }
        }
    }

    private ProtoRawMessageProcessor createStore(
            CradleStorage cradleStorageMock,
            MessageRouter<RawMessageBatch> routerMock,
            Persistor<GroupedMessageBatchToStore> persistor,
            Configuration configuration
    ) {
        return new ProtoRawMessageProcessor(routerMock, cradleStorageMock, persistor, configuration, 0);
    }

    private static RawMessage createMessage(String sessionAlias, String sessionGroup, Direction direction, long sequence, String bookName) {
        return RawMessage.newBuilder()
                .setMetadata(
                        RawMessageMetadata.newBuilder()
                                .setId(createMessageId(Instant.now(), sessionAlias, sessionGroup, direction, sequence, bookName))
                                .build()
                ).setBody(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8)))
                .build();
    }

    private static RawMessage createMessage(String sessionAlias, String sessionGroup, Direction direction, long sequence, Instant timestamp, String bookName) {
        return RawMessage.newBuilder()
                .setMetadata(
                        RawMessageMetadata.newBuilder()
                                .setId(createMessageId(timestamp, sessionAlias, sessionGroup, direction, sequence, bookName))
                                .build()
                ).setBody(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8)))
                .build();
    }

    private static RawMessageBatch createDelivery(Iterable<RawMessage> messages) {
        return RawMessageBatch.newBuilder()
                .addAllMessages(messages)
                .build();
    }

    private static Timestamp extractTimestamp(RawMessageOrBuilder message) {
        return message.getMetadata().getId().getTimestamp();
    }
}