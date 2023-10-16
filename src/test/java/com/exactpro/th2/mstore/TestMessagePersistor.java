/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.CoreStorageSettings;
import com.exactpro.cradle.CradleEntitiesFactory;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.CradleMessage;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.MessageToStoreBuilder;
import com.exactpro.cradle.messages.StoredGroupedMessageBatch;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.taskutils.StartableRunnable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.OngoingStubbing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestMessagePersistor {
    public static final StoredMessage[] EMPTY_STORED_MESSAGE_ARRAY = new StoredMessage[0];
    private final Logger logger = LoggerFactory.getLogger(TestMessagePersistor.class);

    private static final int MAX_MESSAGE_BATCH_SIZE = 16 * 1024;
    private static final int MAX_TEST_EVENT_BATCH_SIZE = 16 * 1024;

    private static final long MESSAGE_PERSIST_TIMEOUT = 100;

    private static final int MAX_MESSAGE_PERSIST_RETRIES = 2;
    private static final int MAX_MESSAGE_QUEUE_TASK_SIZE = 8;
    private static final long MAX_MESSAGE_QUEUE_DATA_SIZE = 10_000L;

    private static final long STORE_ACTION_REJECTION_THRESHOLD = 30000L;
    private static final BookId BOOK_ID = new BookId("test-book");

    private final CradleStorage storageMock = mock(CradleStorage.class);
    private final Callback<GroupedMessageBatchToStore> callback = mock(Callback.class);
    private final ErrorCollector errorCollector = mock(ErrorCollector.class);

    private MessagePersistor persistor;

    private CradleEntitiesFactory cradleObjectsFactory;

    @BeforeEach
    void setUp() throws InterruptedException, CradleStorageException {
        cradleObjectsFactory = spy(new CradleEntitiesFactory(
                MAX_MESSAGE_BATCH_SIZE,
                MAX_TEST_EVENT_BATCH_SIZE,
                new CoreStorageSettings().calculateStoreActionRejectionThreshold()
        ));
        doReturn(CompletableFuture.completedFuture(null)).when(storageMock).storeGroupedMessageBatchAsync(any());

        Configuration config = Configuration.builder()
                .withMaxTaskCount(MAX_MESSAGE_QUEUE_TASK_SIZE)
                .withMaxRetryCount(MAX_MESSAGE_PERSIST_RETRIES)
                .withRetryDelayBase(10L)
                .withMaxTaskDataSize(MAX_MESSAGE_QUEUE_DATA_SIZE)
                .build();
        persistor = spy(new MessagePersistor(errorCollector, storageMock, config));
        persistor.start();
    }

    @AfterEach
    void dispose() {
        logger.info("disposing");
        persistor.close();
        reset(storageMock);
    }


    @Test
    @DisplayName("single raw message persistence")
    public void testSingleRawMessage() throws Exception {

        Instant timestamp = Instant.now();
        String group = "test-group";
        GroupedMessageBatchToStore batch = batchOf(group, createMessage(BOOK_ID, "test-session", Direction.FIRST,
                12, timestamp, "raw message".getBytes()));

        persistor.persist(batch, callback);
        pause(MESSAGE_PERSIST_TIMEOUT);

        ArgumentCaptor<GroupedMessageBatchToStore> capture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
        verify(storageMock, times(1)).storeGroupedMessageBatchAsync(capture.capture());
        verify(persistor, times(1)).processTask(any());
        verify(callback, times(1)).onSuccess(any());
        verify(callback, times(0)).onFail(any());

        GroupedMessageBatchToStore value = capture.getValue();
        assertNotNull(value, "Captured message batch");
        assertStoredGroupMessageBatch(batch, value);
    }


    @Test
    @DisplayName("failed raw message is retried")
    public void testRawMessageResubmitted() throws Exception {

        when(storageMock.storeGroupedMessageBatchAsync(any()))
                .thenReturn(CompletableFuture.failedFuture(new IOException("message persistence failure")))
                .thenReturn(CompletableFuture.completedFuture(null));

        Instant timestamp = Instant.now();
        String group = "test-group";
        GroupedMessageBatchToStore batch = batchOf(group, createMessage(BOOK_ID, "test-session", Direction.FIRST,
                12, timestamp, "raw message".getBytes()));
        persistor.persist(batch, callback);
        pause(MESSAGE_PERSIST_TIMEOUT * 2);

        ArgumentCaptor<GroupedMessageBatchToStore> capture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
        verify(persistor, times(2)).processTask(any());
        verify(storageMock, times(2)).storeGroupedMessageBatchAsync(capture.capture());
        verify(callback, times(1)).onSuccess(any());
        verify(callback, times(0)).onFail(any());

        GroupedMessageBatchToStore value = capture.getValue();
        assertNotNull(value, "Captured stored message batch");
        assertStoredGroupMessageBatch(batch, value);
    }


    @Test
    @DisplayName("failed message is retried limited times")
    public void testEventResubmittedLimitedTimes() throws Exception {

        OngoingStubbing<CompletableFuture<Void>> os = when(storageMock.storeGroupedMessageBatchAsync(any()));
        for (int i = 0; i <= MAX_MESSAGE_PERSIST_RETRIES; i++) {
            os = os.thenReturn(CompletableFuture.failedFuture(new IOException("message persistence failure")));
        }
        os.thenReturn(CompletableFuture.completedFuture(null));

        Instant timestamp = Instant.now();
        String group = "test-group";
        GroupedMessageBatchToStore batch = batchOf(group, createMessage(BOOK_ID, "test-session", Direction.FIRST, 12,
                timestamp, "raw message".getBytes()));
        persistor.persist(batch, callback);
        pause(MESSAGE_PERSIST_TIMEOUT * (MAX_MESSAGE_PERSIST_RETRIES + 1));

        ArgumentCaptor<GroupedMessageBatchToStore> capture = ArgumentCaptor.forClass(GroupedMessageBatchToStore.class);
        verify(persistor, times(MAX_MESSAGE_PERSIST_RETRIES + 1)).processTask(any());
        verify(storageMock, times(MAX_MESSAGE_PERSIST_RETRIES + 1)).storeGroupedMessageBatchAsync(capture.capture());
        verify(callback, times(0)).onSuccess(any());
        verify(callback, times(1)).onFail(any());

        GroupedMessageBatchToStore value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredGroupMessageBatch(batch, value);
    }


    @Test
    @DisplayName("Message persistence is queued by count")
    public void testMessageCountQueueing() throws CradleStorageException, InterruptedException {

        final long storeExecutionTime = MESSAGE_PERSIST_TIMEOUT * 3;
        final long totalExecutionTime = MESSAGE_PERSIST_TIMEOUT * 5;
        final int totalMessages = MAX_MESSAGE_QUEUE_TASK_SIZE + 3;

        // create executor with thread pool size > message queue size to avoid free thread waiting
        ExecutorService executor = Executors.newFixedThreadPool(MAX_MESSAGE_QUEUE_TASK_SIZE * 2);

        Instant timestamp = Instant.now();
        String group = "test-group";
        GroupedMessageBatchToStore[] batch = new GroupedMessageBatchToStore[totalMessages];
        for (int i = 0; i < totalMessages; i++) {
            batch[i] = batchOf(group, createMessage(BOOK_ID, "test-session", Direction.FIRST, i, timestamp,
                    "raw message".getBytes()));
        }

        when(storageMock.storeGroupedMessageBatchAsync(any()))
                .thenAnswer(ignored -> CompletableFuture.runAsync(() -> pause(storeExecutionTime), executor));
        // setup producer thread
        StartableRunnable runnable = StartableRunnable.of(() -> {
            try {
                for (int i = 0; i < totalMessages; i++) {
                    persistor.persist(batch[i], callback);
                }
            } catch (RuntimeException e) {
                logger.error("Exception persisting message batch", e);
                throw new RuntimeException(e);
            }
        });

        new Thread(runnable).start();
        runnable.awaitReadiness();
        runnable.start();

        verify(storageMock, after(MESSAGE_PERSIST_TIMEOUT).times(MAX_MESSAGE_QUEUE_TASK_SIZE))
                .storeGroupedMessageBatchAsync(any());
        verify(storageMock, after(totalExecutionTime).times(totalMessages))
                .storeGroupedMessageBatchAsync(any());
        verify(callback, after(totalExecutionTime).times(totalMessages)).onSuccess(any());
        verify(callback, after(totalExecutionTime).times(0)).onFail(any());

        executor.shutdown();
        executor.awaitTermination(0, TimeUnit.MILLISECONDS);
    }


    @Test
    @DisplayName("Message persistence is queued by message sizes")
    public void testMessageSizeQueueing() throws CradleStorageException, InterruptedException {

        final long storeExecutionTime = MESSAGE_PERSIST_TIMEOUT * 3;
        final long totalExecutionTime = MESSAGE_PERSIST_TIMEOUT * 6;
        final int totalMessages = 5;
        final int messageQueueCapacity = 3;

        // create executor with thread pool size > event queue size to avoid free thread waiting
        ExecutorService executor = Executors.newFixedThreadPool(totalMessages * 2);

        // create events
        final int messageContentSize = (int) (MAX_MESSAGE_QUEUE_DATA_SIZE / messageQueueCapacity * 0.90);
        byte[] content = new byte[messageContentSize];

        Instant timestamp = Instant.now();
        String group = "test-group";
        GroupedMessageBatchToStore[] batch = new GroupedMessageBatchToStore[totalMessages];
        for (int i = 0; i < totalMessages; i++) {
            batch[i] = batchOf(group, createMessage(BOOK_ID, "test-session", Direction.FIRST, i, timestamp, content));
        }

        when(storageMock.storeGroupedMessageBatchAsync(any()))
                .thenAnswer(ignored -> CompletableFuture.runAsync(() -> pause(storeExecutionTime), executor));

        // setup producer thread
        StartableRunnable runnable = StartableRunnable.of(() -> {
            try {
                for (int i = 0; i < totalMessages; i++) {
                    persistor.persist(batch[i], callback);
                }
            } catch (RuntimeException e) {
                logger.error("Exception persisting message batch", e);
                throw new RuntimeException(e);
            }
        });

        new Thread(runnable).start();
        runnable.awaitReadiness();
        runnable.start();

        verify(storageMock, after(MESSAGE_PERSIST_TIMEOUT).times(messageQueueCapacity))
                .storeGroupedMessageBatchAsync(any());
        verify(storageMock, after(totalExecutionTime).times(totalMessages))
                .storeGroupedMessageBatchAsync(any());
        verify(callback, times(totalMessages)).onSuccess(any());
        verify(callback, times(0)).onFail(any());

        executor.shutdown();
        executor.awaitTermination(0, TimeUnit.MILLISECONDS);
    }


    private void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            logger.error("Pause interrupted", e);
        }
    }


    private static MessageToStore createMessage(
            BookId bookId,
            String session,
            Direction direction,
            long sequence,
            Instant timestamp,
            byte[] content
    ) throws CradleStorageException {

        return new MessageToStoreBuilder()
                .bookId(bookId)
                .sessionAlias(session)
                .content(content)
                .timestamp(timestamp)
                .direction(direction)
                .sequence(sequence).build();
    }


    private GroupedMessageBatchToStore batchOf(String group, MessageToStore... messages) throws CradleStorageException {
        GroupedMessageBatchToStore batch = cradleObjectsFactory.groupedMessageBatch(group);
        for (MessageToStore message : messages) {
            batch.addMessage(message);
        }
        return batch;
    }


    private static void assertStoredGroupMessageBatch(StoredGroupedMessageBatch expected, StoredGroupedMessageBatch actual) {
        assertEquals(expected.getGroup(), actual.getGroup());
        assertEquals(expected.getMessageCount(), actual.getMessageCount());
        assertEquals(expected.getFirstTimestamp(), actual.getFirstTimestamp());

        StoredMessage[] expectedMessages = expected.getMessages().toArray(EMPTY_STORED_MESSAGE_ARRAY);
        StoredMessage[] actualMessages = actual.getMessages().toArray(EMPTY_STORED_MESSAGE_ARRAY);
        for (int i = 0; i < expected.getMessageCount(); i++) {
            assertStoredMessage(expectedMessages[i], actualMessages[i]);
        }
    }


    private static void assertStoredMessage(CradleMessage expected, CradleMessage actual) {
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getSessionAlias(), actual.getSessionAlias());
        assertEquals(expected.getDirection(), actual.getDirection());
        assertEquals(expected.getSequence(), actual.getSequence());
        assertEquals(expected.getTimestamp(), actual.getTimestamp());
        assertArrayEquals(expected.getContent(), actual.getContent());
    }
}