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
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.MessageToStoreBuilder;
import com.exactpro.cradle.messages.StoredGroupMessageBatch;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.th2.taskutils.StartableRunnable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestRawMessageBatchPersistor {
    private final Logger logger = LoggerFactory.getLogger(TestRawMessageBatchPersistor.class);

    private static final int  MAX_MESSAGE_BATCH_SIZE      = 16 * 1024;
    private static final int  MAX_TEST_EVENT_BATCH_SIZE   = 16 * 1024;

    private static final long MESSAGE_PERSIST_TIMEOUT       = 100;

    private static final int  MAX_MESSAGE_PERSIST_RETRIES   = 2;
    private static final int  MAX_MESSAGE_QUEUE_TASK_SIZE   = 8;
    private static final long MAX_MESSAGE_QUEUE_DATA_SIZE   = 10_000L;

    private final CradleStorage storageMock = mock(CradleStorage.class);
    private RawMessageBatchPersistor persistor;

    private CradleObjectsFactory cradleObjectsFactory;

    @BeforeEach
    void setUp() throws InterruptedException {
        cradleObjectsFactory = spy(new CradleObjectsFactory(MAX_MESSAGE_BATCH_SIZE, MAX_TEST_EVENT_BATCH_SIZE));
        doReturn(CompletableFuture.completedFuture(null)).when(storageMock).storeGroupedMessageBatchAsync(any(), any());

        Configuration config = Configuration.builder()
                                            .withMaxTaskCount(MAX_MESSAGE_QUEUE_TASK_SIZE)
                                            .withMaxRetryCount(MAX_MESSAGE_PERSIST_RETRIES)
                                            .withRetryDelayBase(10L)
                                            .withMaxTaskDataSize(MAX_MESSAGE_QUEUE_DATA_SIZE)
                                            .build();
        persistor = spy(new RawMessageBatchPersistor(config, storageMock));
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
        StoredGroupMessageBatch batch = batchOf(group, createMessage("test-session", Direction.FIRST, 12, timestamp,
                                                                                    "raw message".getBytes()));

        persistor.persist(batch);
        pause(MESSAGE_PERSIST_TIMEOUT);

        ArgumentCaptor<StoredGroupMessageBatch> capture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
        verify(storageMock, times(1)).storeGroupedMessageBatchAsync(capture.capture(), Mockito.eq(group));
        verify(persistor, times(1)).processTask(any());

        StoredGroupMessageBatch value = capture.getValue();
        assertNotNull(value, "Captured message batch");
        assertStoredGroupMessageBatch(batch, value);
    }


    @Test
    @DisplayName("failed raw message is retried")
    public void testRawMessageResubmitted() throws Exception {

        when(storageMock.storeGroupedMessageBatchAsync(any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new IOException("message persistence failure")))
                .thenReturn(CompletableFuture.completedFuture(null));

        Instant timestamp = Instant.now();
        String group = "test-group";
        StoredGroupMessageBatch batch = batchOf(group, createMessage("test-session", Direction.FIRST, 12, timestamp,
                                                            "raw message".getBytes()));
        persistor.persist(batch);
        pause(MESSAGE_PERSIST_TIMEOUT * 2);

        ArgumentCaptor<StoredGroupMessageBatch> capture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
        verify(persistor, times(2)).processTask(any());
        verify(storageMock, times(2)).storeGroupedMessageBatchAsync(capture.capture(), Mockito.eq(group));

        StoredGroupMessageBatch value = capture.getValue();
        assertNotNull(value, "Captured stored message batch");
        assertStoredGroupMessageBatch(batch, value);
    }


    @Test
    @DisplayName("failed message is retried limited times")
    public void testEventResubmittedLimitedTimes() throws Exception {

        OngoingStubbing<CompletableFuture<Void>> os = when(storageMock.storeGroupedMessageBatchAsync(any(), any()));
        for (int i = 0; i <= MAX_MESSAGE_PERSIST_RETRIES; i++)
            os = os.thenReturn(CompletableFuture.failedFuture(new IOException("message persistence failure")));
        os.thenReturn(CompletableFuture.completedFuture(null));

        Instant timestamp = Instant.now();
        String group = "test-group";
        StoredGroupMessageBatch batch = batchOf(group, createMessage("test-session", Direction.FIRST, 12, timestamp,
                                                                        "raw message".getBytes()));
        persistor.persist(batch);
        pause(MESSAGE_PERSIST_TIMEOUT * (MAX_MESSAGE_PERSIST_RETRIES + 1));

        ArgumentCaptor<StoredGroupMessageBatch> capture = ArgumentCaptor.forClass(StoredGroupMessageBatch.class);
        verify(persistor, times(MAX_MESSAGE_PERSIST_RETRIES + 1)).processTask(any());
        verify(storageMock, times(MAX_MESSAGE_PERSIST_RETRIES + 1)).storeGroupedMessageBatchAsync(capture.capture(), Mockito.eq(group));

        StoredGroupMessageBatch value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredGroupMessageBatch(batch, value);
    }


    @Test
    @DisplayName("Message persistence is queued by count")
    public void testMessageCountQueueing() throws Exception {

        final long storeExecutionTime = MESSAGE_PERSIST_TIMEOUT * 3;
        final long totalExecutionTime = MESSAGE_PERSIST_TIMEOUT * 5;
        final int totalMessages = MAX_MESSAGE_QUEUE_TASK_SIZE + 3;

        // create executor with thread pool size > message queue size to avoid free thread waiting
        final ExecutorService executor = Executors.newFixedThreadPool(MAX_MESSAGE_QUEUE_TASK_SIZE * 2);

        Instant timestamp = Instant.now();
        String group = "test-group";
        StoredGroupMessageBatch[] batch = new StoredGroupMessageBatch[totalMessages];
        for (int i = 0; i < totalMessages; i++)
            batch[i] = batchOf(group, createMessage("test-session", Direction.FIRST, i, timestamp, "raw message".getBytes()));

        when(storageMock.storeGroupedMessageBatchAsync(any(), any()))
                .thenAnswer((ignored) ->  CompletableFuture.runAsync(() -> pause(storeExecutionTime), executor));
        // setup producer thread
        StartableRunnable runnable = StartableRunnable.of(() -> {
            try {
                for (int i = 0; i < totalMessages; i++)
                    persistor.persist(batch[i]);
            } catch (Exception e) {
                logger.error("Exception persisting message batch", e);
                throw new RuntimeException(e);
            }
        });

        new Thread(runnable).start();
        runnable.awaitReadiness();
        runnable.start();

        verify(storageMock, after(MESSAGE_PERSIST_TIMEOUT).times(MAX_MESSAGE_QUEUE_TASK_SIZE))
                .storeGroupedMessageBatchAsync(any(), Mockito.eq(group));
        verify(storageMock, after(totalExecutionTime).times(totalMessages))
                .storeGroupedMessageBatchAsync(any(), Mockito.eq(group));

        executor.shutdown();
        executor.awaitTermination(0, TimeUnit.MILLISECONDS);
    }


    @Test
    @DisplayName("Message persistence is queued by message sizes")
    public void testMessageSizeQueueing() throws Exception {

        final long storeExecutionTime = MESSAGE_PERSIST_TIMEOUT * 3;
        final long totalExecutionTime = MESSAGE_PERSIST_TIMEOUT * 6;
        final int totalMessages = 5;
        final int messageCapacityInQueue = 3;

        // create executor with thread pool size > event queue size to avoid free thread waiting
        final ExecutorService executor = Executors.newFixedThreadPool(totalMessages * 2);

        // create events
        final int messageContentSize = (int) (MAX_MESSAGE_QUEUE_DATA_SIZE / messageCapacityInQueue * 0.90);
        final byte[] content = new byte[messageContentSize];

        Instant timestamp = Instant.now();
        String group = "test-group";
        StoredGroupMessageBatch[] batch = new StoredGroupMessageBatch[totalMessages];
        for (int i = 0; i < totalMessages; i++)
            batch[i] = batchOf(group, createMessage("test-session", Direction.FIRST, i, timestamp, content));

        when(storageMock.storeGroupedMessageBatchAsync(any(), any()))
                .thenAnswer((ignored) ->  CompletableFuture.runAsync(() ->  pause(storeExecutionTime), executor));

        // setup producer thread
        StartableRunnable runnable = StartableRunnable.of(() -> {
            try {
                for (int i = 0; i < totalMessages; i++)
                    persistor.persist(batch[i]);
            } catch (Exception e) {
                logger.error("Exception persisting message batch", e);
                throw new RuntimeException(e);
            }
        });

        new Thread(runnable).start();
        runnable.awaitReadiness();
        runnable.start();

        verify(storageMock, after(MESSAGE_PERSIST_TIMEOUT).times(messageCapacityInQueue))
                .storeGroupedMessageBatchAsync(any(), Mockito.eq(group));
        verify(storageMock, after(totalExecutionTime).times(totalMessages))
                .storeGroupedMessageBatchAsync(any(), Mockito.eq(group));

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


    private MessageToStore createMessage(String session, Direction direction, long sequence, Instant timestamp,
                                              byte[] content) {

        return new MessageToStoreBuilder()
                .streamName(session)
                .content(content)
                .timestamp(timestamp)
                .direction(direction)
                .index(sequence).build();
    }


    private StoredGroupMessageBatch batchOf(String group, MessageToStore... messages) throws Exception {
        StoredGroupMessageBatch batch = cradleObjectsFactory.createGroupMessageBatch(group);
        for (MessageToStore message : messages)
            batch.addMessage(message);
        return batch;
    }


    private void assertStoredGroupMessageBatch(StoredGroupMessageBatch expected, StoredGroupMessageBatch actual) {
        assertEquals(expected.getSessionGroup(), actual.getSessionGroup());
        assertEquals(expected.getMessageCount(), actual.getMessageCount());
        assertEquals(expected.getFirstTimestamp(), actual.getFirstTimestamp());

        StoredMessage[] expectedMessages = expected.getMessages().toArray(new StoredMessage[0]);
        StoredMessage[] actualMessages = actual.getMessages().toArray(new StoredMessage[0]);
        for (int i = 0; i < expected.getMessageCount(); i++)
            assertStoredMessage(expectedMessages[i], actualMessages[i]);
    }


    private void assertStoredMessage(StoredMessage expected, StoredMessage actual) {
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getStreamName(), actual.getStreamName());
        assertEquals(expected.getDirection(), actual.getDirection());
        assertEquals(expected.getIndex(), actual.getIndex());
        assertEquals(expected.getTimestamp(), actual.getTimestamp());
        assertArrayEquals(expected.getContent(), actual.getContent());
    }
}