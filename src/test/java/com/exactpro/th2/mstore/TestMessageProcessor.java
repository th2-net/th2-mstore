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

import com.exactpro.cradle.CradleEntitiesFactory;
import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.cassandra.resultset.CassandraCradleResultSet;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.*;
import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampOrBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Random;

import static com.exactpro.th2.common.event.EventUtils.toTimestamp;
import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class TestMessageProcessor {
    private static final int DRAIN_TIMEOUT = 1000;
    private static final int TEST_MESSAGE_BATCH_SIZE = 1024;
    private static final int TEST_EVENT_BATCH_SIZE = CradleStorage.DEFAULT_MAX_MESSAGE_BATCH_SIZE;

    private final CradleManager cradleManagerMock = mock(CradleManager.class);
    private final CradleStorage storageMock = mock(CradleStorage.class);
    @SuppressWarnings("unchecked")
    private final MessageRouter<RawMessageBatch> routerMock = (MessageRouter<RawMessageBatch>) mock(MessageRouter.class);

    @SuppressWarnings("unchecked")
    private final Persistor persistor = mock(Persistor.class);
    private final DeliveryMetadata deliveryMetadata = new DeliveryMetadata("", false);
    private final ManualAckDeliveryCallback.Confirmation confirmation = mock(ManualAckDeliveryCallback.Confirmation.class);

    private final Random random = new Random();

    private MessageProcessor messageStore;

    private CradleEntitiesFactory cradleEntitiesFactory;


    @BeforeEach
    void setUp() throws CradleStorageException, IOException {
        cradleEntitiesFactory = spy(new CradleEntitiesFactory(TEST_MESSAGE_BATCH_SIZE, TEST_EVENT_BATCH_SIZE));

        CassandraCradleResultSet rsMock = mock(CassandraCradleResultSet.class);
        when(rsMock.next()).thenReturn(null);
        when(storageMock.getMessages(any())).thenReturn(rsMock);

        when(storageMock.getEntitiesFactory()).thenReturn(cradleEntitiesFactory);
        when(cradleManagerMock.getStorage()).thenReturn(storageMock);
        when(routerMock.subscribeAllWithManualAck(any(), any(), any())).thenReturn(mock(SubscriberMonitor.class));
        Configuration configuration = Configuration.builder().withDrainInterval(DRAIN_TIMEOUT / 10).build();
        messageStore = spy(createStore(storageMock, routerMock, persistor, configuration));
        messageStore.start();
    }

    @AfterEach
    void tearDown() {
        messageStore.close();
    }


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
    private RawMessageBatch deliveryOf(RawMessage... messages) {
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
        void testEmptyDelivery() throws Exception {
            messageStore.process(deliveryMetadata, deliveryOf(), confirmation);
            verify(persistor, never()).persist(any(), any());
        }

        @Test
        @DisplayName("Delivery with unordered sequences is not stored")
        void testUnorderedDelivery() throws Exception {
            String bookName = bookName(random.nextInt());
            RawMessage first = createMessage("test", "group", Direction.FIRST, 1, bookName);
            RawMessage second = createMessage("test", "group",  Direction.FIRST, 2, bookName);

            messageStore.process(deliveryMetadata, deliveryOf(second, first), confirmation);
            verify(persistor, never()).persist(any(), any());
        }

        @Test
        @DisplayName("Duplicated delivery is ignored")
        void testDuplicatedDelivery() throws Exception {
            String bookName = bookName(random.nextInt());

            RawMessage first = createMessage("test", "group", Direction.FIRST, 1, bookName);
            messageStore.process(deliveryMetadata, deliveryOf(first), confirmation);

            RawMessage duplicate = createMessage("test", "group", Direction.FIRST, 1, bookName);
            messageStore.process(deliveryMetadata, deliveryOf(duplicate), confirmation);

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
        void testDifferentDirectionDelivery() throws Exception {
            String bookName = bookName(random.nextInt());

            RawMessage first = createMessage("testA", "group", Direction.FIRST, 1, bookName);
            messageStore.process(deliveryMetadata, deliveryOf(first), confirmation);

            RawMessage duplicate = createMessage("testB", "group", Direction.SECOND, 1, bookName);
            messageStore.process(deliveryMetadata, deliveryOf(duplicate), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            verify(persistor, timeout(DRAIN_TIMEOUT).times(1)).persist(batchCapture.capture(), any());
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
        void testSingleMessageDelivery() throws Exception {
            String bookName = bookName(random.nextInt());
            RawMessage first = createMessage("test", "group", Direction.FIRST, 1, bookName);

            messageStore.process(deliveryMetadata, deliveryOf(first), confirmation);

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
        void testNormalDelivery() throws Exception {
            String bookName = bookName(random.nextInt());
            RawMessage first = createMessage("test", "group", Direction.FIRST, 1, bookName);
            RawMessage second = createMessage("test", "group", Direction.FIRST, 2, bookName);

            messageStore.process(deliveryMetadata, deliveryOf(first, second), confirmation);

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
        void joinsBatches() throws Exception {
            String bookName = bookName(random.nextInt());
            RawMessage first = createMessage("test", "group", Direction.FIRST, 1, bookName);
            RawMessage second = createMessage("test", "group", Direction.FIRST, 2, bookName);

            messageStore.process(deliveryMetadata, deliveryOf(first), confirmation);
            messageStore.process(deliveryMetadata, deliveryOf(second), confirmation);

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
        void rejectsDecreasingTimestamps () throws Exception {
            String bookName = bookName(random.nextInt());
            RawMessage secondToProcess = createMessage("test", "group", Direction.FIRST, 2, bookName);
            RawMessage firstToProcess = createMessage("test", "group", Direction.FIRST, 1, bookName);

            messageStore.process(deliveryMetadata, deliveryOf(firstToProcess), confirmation);
            messageStore.process(deliveryMetadata, deliveryOf(secondToProcess), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);

            verify(persistor, timeout(DRAIN_TIMEOUT).times(1)).persist(batchCapture.capture(), any());
            verify(confirmation, timeout(DRAIN_TIMEOUT).times(1)).reject();

        }
    }

    @Test
    @DisplayName("Delivery with different aliases is stored")
    void testDifferentAliases() throws Exception {
        String bookName = bookName(random.nextInt());
        RawMessage first = createMessage("testA", "group", Direction.FIRST, 1, bookName);
        RawMessage second = createMessage("testB", "group", Direction.FIRST, 2, bookName);

        messageStore.process(deliveryMetadata, deliveryOf(first, second), confirmation);
        verify(persistor, timeout(DRAIN_TIMEOUT).times(1)).persist(any(), any());
    }

    @Test
    @DisplayName("Delivery with different directions is stored")
    void testDifferentDirections() throws Exception {
        String bookName = bookName(random.nextInt());
        RawMessage first = createMessage("test", "group", Direction.FIRST, 1, bookName);
        RawMessage second = createMessage("test", "group", Direction.SECOND, 2, bookName);

        messageStore.process(deliveryMetadata, deliveryOf(first, second), confirmation);
        verify(persistor, timeout(DRAIN_TIMEOUT).times(1)).persist(any(), any());
    }


    @Nested
    @DisplayName("Deliveries for different groups")
    class TestDeliveriesForDifferentGroups {
        @Test
        @DisplayName("Deliveries for different groups are stored separately")
        void separateBatches() throws Exception {
            String bookName = bookName(random.nextInt());
            RawMessage first = createMessage("testA", "group1", Direction.FIRST, 1, bookName);
            RawMessage second = createMessage("testB", "group2", Direction.FIRST, 2, bookName);

            messageStore.process(deliveryMetadata, deliveryOf(first), confirmation);
            messageStore.process(deliveryMetadata, deliveryOf(second), confirmation);

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

    private MessageProcessor createStore(
            CradleStorage cradleStorageMock,
            MessageRouter<RawMessageBatch> routerMock,
            Persistor<GroupedMessageBatchToStore> persistor,
            Configuration configuration
    ) {
        return new MessageProcessor(routerMock, cradleStorageMock, persistor, configuration);
    }

    private RawMessage createMessage(String sessionAlias, String sessionGroup, Direction direction, long sequence, String bookName) {
        return RawMessage.newBuilder()
                .setMetadata(
                        RawMessageMetadata.newBuilder()
                                .setId(createMessageId(Instant.now(), sessionAlias, sessionGroup, direction, sequence, bookName))
                                .build()
                ).setBody(ByteString.copyFrom("test".getBytes(StandardCharsets.UTF_8)))
                .build();
    }

    private RawMessageBatch createDelivery(List<RawMessage> messages) {
        return RawMessageBatch.newBuilder()
                .addAllMessages(messages)
                .build();
    }

    private Timestamp extractTimestamp(RawMessage message) {
        return message.getMetadata().getId().getTimestamp();
    }
}