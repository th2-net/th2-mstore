/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.TransportUtilsKt;
import io.netty.buffer.Unpooled;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static com.exactpro.th2.mstore.TransportGroupProcessor.toCradleDirection;
import static com.exactpro.th2.mstore.TransportGroupProcessor.toCradleMessage;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestTransportGroupProcessor {
    private static final int DRAIN_TIMEOUT = 1000;
    private static final int TEST_MESSAGE_BATCH_SIZE = 1024;
    private static final int TEST_EVENT_BATCH_SIZE = CradleStorage.DEFAULT_MAX_MESSAGE_BATCH_SIZE;

    private final CradleManager cradleManagerMock = mock(CradleManager.class);
    private final CradleStorage storageMock = mock(CradleStorage.class);
    @SuppressWarnings("unchecked")
    private final MessageRouter<GroupBatch> routerMock = (MessageRouter<GroupBatch>) mock(MessageRouter.class);

    @SuppressWarnings("SpellCheckingInspection")
    private final MessagePersistor persistor = mock(MessagePersistor.class);
    private final DeliveryMetadata deliveryMetadata = new DeliveryMetadata("", false);
    private final ManualAckDeliveryCallback.Confirmation confirmation = mock(ManualAckDeliveryCallback.Confirmation.class);

    private final Random random = new Random();

    private TransportGroupProcessor messageStore;

    private CradleEntitiesFactory cradleEntitiesFactory;


    @BeforeEach
    void setUp() throws CradleStorageException, IOException {
        cradleEntitiesFactory = spy(new CradleEntitiesFactory(
                TEST_MESSAGE_BATCH_SIZE,
                TEST_EVENT_BATCH_SIZE,
                new CoreStorageSettings().calculateStoreActionRejectionThreshold()
        ));

        //noinspection unchecked
        CassandraCradleResultSet<StoredMessage> rsMock = mock(CassandraCradleResultSet.class);
        when(rsMock.next()).thenReturn(null);
        when(storageMock.getMessages(any())).thenReturn(rsMock);

        when(storageMock.getEntitiesFactory()).thenReturn(cradleEntitiesFactory);
        when(cradleManagerMock.getStorage()).thenReturn(storageMock);
        when(routerMock.subscribeAllWithManualAck(any())).thenReturn(mock(SubscriberMonitor.class));
        Configuration configuration = Configuration.builder().withDrainInterval(DRAIN_TIMEOUT / 10).build();
        messageStore = spy(createStore(storageMock, routerMock, persistor, configuration));
        messageStore.start();
    }

    @Test
    public void testToCradleMessage() throws CradleStorageException {
        String book = "test-book";
        RawMessage rawMessage = new RawMessage(
                new MessageId(
                        "test-session-alias",
                        Direction.OUTGOING,
                        6234L,
                        Collections.emptyList(),
                        Instant.now()
                ),
                null,
                Map.of("test-key", "test-value"),
                "test-protocol",
                Unpooled.wrappedBuffer(new byte[]{1, 4, 2, 3})
        );

        MessageToStore cradleMessage = toCradleMessage(book, rawMessage);

        Assertions.assertEquals(rawMessage.getProtocol(), cradleMessage.getProtocol());
        Assertions.assertEquals(rawMessage.getMetadata(), cradleMessage.getMetadata().toMap());
        Assertions.assertEquals(rawMessage.getId().getSessionAlias(), cradleMessage.getSessionAlias());
        Assertions.assertEquals(book, cradleMessage.getBookId().getName());
        Assertions.assertEquals(toCradleDirection(rawMessage.getId().getDirection()), cradleMessage.getDirection());
        Assertions.assertEquals(rawMessage.getId().getTimestamp(), cradleMessage.getTimestamp());
        Assertions.assertEquals(rawMessage.getId().getSequence(), cradleMessage.getSequence());
        Assertions.assertArrayEquals(TransportUtilsKt.toByteArray(rawMessage.getBody()), cradleMessage.getContent());
    }

    @AfterEach
    void tearDown() {
        messageStore.close();
    }


    private static String bookName(int i) {
        return "book-name-" + i;
    }

    private GroupBatch deliveryOf(String book, String sessionGroup, RawMessage... messages) {
        return createDelivery(book, sessionGroup, List.of(messages));
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
            if (!(values == null || values.size() == 0))
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
        void testEmptyDelivery() {
            messageStore.process(deliveryMetadata, deliveryOf("book", "group"), confirmation);
            verify(persistor, never()).persist(any(), any());
        }

        @Test
        @DisplayName("Delivery with unordered sequences is not stored")
        void testUnorderedDelivery() {
            String bookName = bookName(random.nextInt());
            RawMessage first = createMessage("test", Direction.INCOMING, 1);
            RawMessage second = createMessage("test", Direction.INCOMING, 2);

            messageStore.process(deliveryMetadata, deliveryOf(bookName, "group", second, first), confirmation);
            verify(persistor, never()).persist(any(), any());
        }

        @Test
        @DisplayName("Duplicated delivery is ignored")
        void testDuplicatedDelivery() {
            String bookName = bookName(random.nextInt());

            RawMessage first = createMessage("test", Direction.INCOMING, 1);
            messageStore.process(deliveryMetadata, deliveryOf(bookName, "group", first), confirmation);

            RawMessage duplicate = createMessage("test", Direction.INCOMING, 1);
            messageStore.process(deliveryMetadata, deliveryOf(bookName, "group", duplicate), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            verify(persistor, timeout(DRAIN_TIMEOUT)).persist(batchCapture.capture(), any());
            verify(cradleEntitiesFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/))
                    .groupedMessageBatch(groupCapture.capture());

            assertEquals(1, batchCapture.getAllValues().size());

            GroupedMessageBatchToStore batchValue = batchCapture.getValue();
            assertNotNull(batchValue);
            assertMessageBatchToStore(batchValue, bookName, "group", 1);
            assertEquals(extractTimestamp(first), batchValue.getLastTimestamp());
        }
    }

    @Nested
    @DisplayName("Handling single delivery")
    class TestSingleDelivery {

        @Test
        @DisplayName("Different sessions can have the same sequence")
        void testDifferentDirectionDelivery() {
            String bookName = bookName(random.nextInt());

            RawMessage first = createMessage("testA", Direction.INCOMING, 1);
            messageStore.process(deliveryMetadata, deliveryOf(bookName, "group", first), confirmation);

            RawMessage duplicate = createMessage("testB", Direction.OUTGOING, 1);
            messageStore.process(deliveryMetadata, deliveryOf(bookName, "group", duplicate), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            verify(persistor, timeout(DRAIN_TIMEOUT).times(1)).persist(batchCapture.capture(), any());
            int invocations = 2 + 2 /*invocations in SessionBatchHolder (init + reset)*/;
            verify(cradleEntitiesFactory, times(invocations)).groupedMessageBatch(groupCapture.capture());

            List<GroupedMessageBatchToStore> value = batchCapture.getAllValues();
            assertNotNull(value);
            assertEquals(1, value.size());

            GroupedMessageBatchToStore batch = value.get(0);

            boolean a = false, b = false;
            List<StoredMessage> messages = List.copyOf(batch.getMessages());
            assertEquals(2, messages.size());
            for (StoredMessage m : messages) {
                if (m.getDirection() == toCradleDirection(Direction.INCOMING)) {
                    assertMessageToStore(m, bookName, "testA", Direction.INCOMING);
                    a = true;
                } else {
                    assertMessageToStore(m, bookName, "testB", Direction.OUTGOING);
                    b = true;
                }
            }
            assertTrue(a);
            assertTrue(b);

            assertAllGroupValuesMatchTo(groupCapture, "group", 4);
        }

        @Test
        @DisplayName("Delivery with single message is stored normally")
        void testSingleMessageDelivery() {
            String bookName = bookName(random.nextInt());
            RawMessage first = createMessage("test", Direction.INCOMING, 1);

            messageStore.process(deliveryMetadata, deliveryOf(bookName, "group", first), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            verify(persistor, timeout(DRAIN_TIMEOUT)).persist(batchCapture.capture(), any());
            verify(cradleEntitiesFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/))
                    .groupedMessageBatch(groupCapture.capture());

            GroupedMessageBatchToStore value = batchCapture.getValue();
            assertNotNull(value);
            assertEquals(1, value.getMessageCount());
            assertMessageToStore(List.copyOf(value.getMessages()).get(0), bookName, "test", Direction.INCOMING);

            assertAllGroupValuesMatchTo(groupCapture, "group", 3);
        }

        @Test
        @DisplayName("Delivery with ordered messages for one session are stored")
        void testNormalDelivery() {
            String bookName = bookName(random.nextInt());
            RawMessage first = createMessage("test", Direction.INCOMING, 1);
            RawMessage second = createMessage("test", Direction.INCOMING, 2);

            messageStore.process(deliveryMetadata, deliveryOf(bookName, "group", first, second), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            verify(persistor, timeout(DRAIN_TIMEOUT)).persist(batchCapture.capture(), any());
            verify(cradleEntitiesFactory, times(1 + 2/*invocations in SessionBatchHolder (init + reset)*/))
                    .groupedMessageBatch(groupCapture.capture());

            GroupedMessageBatchToStore value = batchCapture.getValue();
            assertNotNull(value);
            assertEquals(2, value.getMessageCount());
            List<StoredMessage> messages = List.copyOf(value.getMessages());
            assertMessageToStore(messages.get(0), bookName, "test", Direction.INCOMING);
            assertMessageToStore(messages.get(1), bookName, "test", Direction.INCOMING);

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
            RawMessage first = createMessage("test", Direction.INCOMING, 1);
            RawMessage second = createMessage("test", Direction.INCOMING, 2);

            messageStore.process(deliveryMetadata, deliveryOf(bookName, "group", first), confirmation);
            messageStore.process(deliveryMetadata, deliveryOf(bookName, "group", second), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
            ArgumentCaptor<String> groupCapture = ArgumentCaptor.forClass(String.class);
            verify(persistor, timeout(DRAIN_TIMEOUT)).persist(batchCapture.capture(), any());
            verify(cradleEntitiesFactory, times(2 + 2/*invocations in SessionBatchHolder (init + reset)*/))
                    .groupedMessageBatch(groupCapture.capture());

            GroupedMessageBatchToStore value = batchCapture.getValue();
            assertNotNull(value);
            assertEquals(2, value.getMessageCount());
            List<StoredMessage> messages = List.copyOf(value.getMessages());
            assertMessageToStore(messages.get(0), bookName, "test", Direction.INCOMING);
            assertMessageToStore(messages.get(1), bookName, "test", Direction.INCOMING);

            assertAllGroupValuesMatchTo(groupCapture, "group", 4);
        }

        @Test
        @DisplayName("Several deliveries have one session, increasing sequences, but decreasing timestamps")
        void rejectsDecreasingTimestamps() throws Exception {
            String bookName = bookName(random.nextInt());
            RawMessage secondToProcess = createMessage("test", Direction.INCOMING, 2);
            RawMessage firstToProcess = createMessage("test", Direction.INCOMING, 1);

            messageStore.process(deliveryMetadata, deliveryOf(bookName, "group", firstToProcess), confirmation);
            messageStore.process(deliveryMetadata, deliveryOf(bookName, "group", secondToProcess), confirmation);

            ArgumentCaptor<GroupedMessageBatchToStore> batchCapture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);

            verify(persistor, timeout(DRAIN_TIMEOUT).times(1)).persist(batchCapture.capture(), any());
            verify(confirmation, timeout(DRAIN_TIMEOUT).times(1)).reject();

        }
    }

    @Test
    @DisplayName("Delivery with different aliases is stored")
    void testDifferentAliases() {
        String bookName = bookName(random.nextInt());
        RawMessage first = createMessage("testA", Direction.INCOMING, 1);
        RawMessage second = createMessage("testB", Direction.INCOMING, 2);

        messageStore.process(deliveryMetadata, deliveryOf(bookName, "group", first, second), confirmation);
        verify(persistor, timeout(DRAIN_TIMEOUT).times(1)).persist(any(), any());
    }

    @Test
    @DisplayName("Delivery with different directions is stored")
    void testDifferentDirections() {
        String bookName = bookName(random.nextInt());
        RawMessage first = createMessage("test", Direction.INCOMING, 1);
        RawMessage second = createMessage("test", Direction.OUTGOING, 2);

        messageStore.process(deliveryMetadata, deliveryOf(bookName, "group", first, second), confirmation);
        verify(persistor, timeout(DRAIN_TIMEOUT).times(1)).persist(any(), any());
    }


    @Nested
    @DisplayName("Deliveries for different groups")
    class TestDeliveriesForDifferentGroups {
        @Test
        @DisplayName("Deliveries for different groups are stored separately")
        void separateBatches() {
            String bookName = bookName(random.nextInt());
            RawMessage first = createMessage("testA", Direction.INCOMING, 1);
            RawMessage second = createMessage("testB", Direction.INCOMING, 2);

            messageStore.process(deliveryMetadata, deliveryOf(bookName, "group1", first), confirmation);
            messageStore.process(deliveryMetadata, deliveryOf(bookName, "group2", second), confirmation);

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
                if (group.equals("group1")) {
                    List<StoredMessage> messages = List.copyOf(batch.getMessages());
                    assertMessageToStore(messages.get(0), bookName, "testA", Direction.INCOMING);
                    assertEquals(1, messages.get(0).getSequence());
                }
                if (group.equals("group2")) {
                    List<StoredMessage> messages = List.copyOf(batch.getMessages());
                    assertMessageToStore(messages.get(0), bookName, "testB", Direction.INCOMING);
                    assertEquals(2, messages.get(0).getSequence());
                }
            }
        }
    }

    private TransportGroupProcessor createStore(
            CradleStorage cradleStorageMock,
            MessageRouter<GroupBatch> routerMock,
            Persistor<GroupedMessageBatchToStore> persistor,
            Configuration configuration
    ) {
        return new TransportGroupProcessor(routerMock, cradleStorageMock, persistor, configuration, 0);
    }

    private RawMessage createMessage(String sessionAlias, Direction direction, long sequence) {
        return new RawMessage(
                new MessageId(sessionAlias, direction, sequence, emptyList(), Instant.now()),
                null,
                emptyMap(),
                "",
                Unpooled.wrappedBuffer("test".getBytes(StandardCharsets.UTF_8))
        );
    }

    private GroupBatch createDelivery(String book, String sessionGroup, List<RawMessage> messages) {
        return new GroupBatch(book, sessionGroup, messages.stream()
                .map(msg -> new MessageGroup(List.of(msg)))
                .collect(Collectors.toList()));
    }

    private Instant extractTimestamp(RawMessage message) {
        return message.getId().getTimestamp();
    }
}