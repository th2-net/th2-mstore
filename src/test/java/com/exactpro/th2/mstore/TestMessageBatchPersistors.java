package com.exactpro.th2.mstore;

import com.exactpro.cradle.CradleObjectsFactory;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.MessageToStoreBuilder;
import com.exactpro.cradle.messages.StoredMessage;
import com.exactpro.cradle.messages.StoredMessageBatch;
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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class TestMessageBatchPersistors {
    private final Logger logger = LoggerFactory.getLogger(TestMessageBatchPersistors.class);

    private static final int  MAX_MESSAGE_BATCH_SIZE      = 16 * 1024;
    private static final int  MAX_TEST_EVENT_BATCH_SIZE   = 16 * 1024;

    private static final long MESSAGE_PERSIST_TIMEOUT       = 100;

    private static final int  MAX_MESSAGE_PERSIST_RETRIES   = 2;
    private static final int  MAX_MESSAGE_QUEUE_TASK_SIZE   = 8;
    private static final long MAX_MESSAGE_QUEUE_DATA_SIZE   = 10_000L;

    private final CradleStorage storageMock = mock(CradleStorage.class);
    private MessageBatchPersistors persistors;

    private CradleObjectsFactory cradleObjectsFactory;

    @BeforeEach
    void setUp() throws InterruptedException {
        cradleObjectsFactory = spy(new CradleObjectsFactory(MAX_MESSAGE_BATCH_SIZE, MAX_TEST_EVENT_BATCH_SIZE));
        doReturn(CompletableFuture.completedFuture(null)).when(storageMock).storeMessageBatchAsync(any());
        doReturn(CompletableFuture.completedFuture(null)).when(storageMock).storeProcessedMessageBatchAsync(any());

        Configuration config = Configuration.builder()
                                            .withMaxTaskCount(MAX_MESSAGE_QUEUE_TASK_SIZE)
                                            .withMaxRetryCount(MAX_MESSAGE_PERSIST_RETRIES)
                                            .withRetryDelayBase(10L)
                                            .withMaxTaskDataSize(MAX_MESSAGE_QUEUE_DATA_SIZE)
                                            .build();
        persistors = spy(new MessageBatchPersistors(config, storageMock));
        persistors.start();
    }

    @AfterEach
    void dispose() {
        logger.info("disposing");
        persistors.close();
        reset(storageMock);
    }


    @Test
    @DisplayName("single raw message persistence")
    public void testSingleRawMessage() throws Exception {

        Instant timestamp = Instant.now();
        StoredMessageBatch batch = batchOf(createMessage("test-session", Direction.FIRST, 12, timestamp,
                "raw message".getBytes()));

        persistors.getRawPersistor().persist(batch);

        pause(MESSAGE_PERSIST_TIMEOUT);

        ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
        verify(storageMock, times(1)).storeMessageBatchAsync(capture.capture());
        verify(persistors, times(1)).processTask(any());

        StoredMessageBatch value = capture.getValue();
        assertNotNull(value, "Captured message batch");
        assertStoredMessageBatch(batch, value);
    }

    @Test
    @DisplayName("single parsed message persistence")
    public void testSingleParsedMessage() throws Exception {

        Instant timestamp = Instant.now();
        StoredMessageBatch batch = batchOf(createMessage("test-session", Direction.SECOND, 34, timestamp,
                "parsed message".getBytes()));

        persistors.getParsedPersistor().persist(batch);

        pause(MESSAGE_PERSIST_TIMEOUT);

        ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
        verify(storageMock, times(1)).storeProcessedMessageBatchAsync(capture.capture());
        verify(persistors, times(1)).processTask(any());

        StoredMessageBatch value = capture.getValue();
        assertNotNull(value, "Captured message batch");
        assertStoredMessageBatch(batch, value);
    }

    @Test
    @DisplayName("failed raw message is retried")
    public void testRawMessageResubmitted() throws Exception {

        when(storageMock.storeMessageBatchAsync(any()))
                .thenReturn(CompletableFuture.failedFuture(new IOException("message persistence failure")))
                .thenReturn(CompletableFuture.completedFuture(null));

        Instant timestamp = Instant.now();
        StoredMessageBatch batch = batchOf(createMessage("test-session", Direction.FIRST, 12, timestamp,
                "raw message".getBytes()));
        persistors.getRawPersistor().persist(batch);

        pause(MESSAGE_PERSIST_TIMEOUT * 2);

        ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
        verify(persistors, times(2)).processTask(any());
        verify(storageMock, times(2)).storeMessageBatchAsync(capture.capture());

        StoredMessageBatch value = capture.getValue();
        assertNotNull(value, "Captured stored message batch");
        assertStoredMessageBatch(batch, value);
    }

    @Test
    @DisplayName("failed parsed message is retried")
    public void testParsedMessageResubmitted() throws Exception {

        when(storageMock.storeProcessedMessageBatchAsync(any()))
                .thenReturn(CompletableFuture.failedFuture(new IOException("message persistence failure")))
                .thenReturn(CompletableFuture.completedFuture(null));

        Instant timestamp = Instant.now();
        StoredMessageBatch batch = batchOf(createMessage("test-session", Direction.FIRST, 12, timestamp,
                "parsed message".getBytes()));
        persistors.getParsedPersistor().persist(batch);

        pause(MESSAGE_PERSIST_TIMEOUT * 2);

        ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
        verify(persistors, times(2)).processTask(any());
        verify(storageMock, times(2)).storeProcessedMessageBatchAsync(capture.capture());

        StoredMessageBatch value = capture.getValue();
        assertNotNull(value, "Captured stored message batch");
        assertStoredMessageBatch(batch, value);
    }


    @Test
    @DisplayName("failed message is retried limited times")
    public void testEventResubmittedLimitedTimes() throws Exception {

        OngoingStubbing<CompletableFuture<Void>> os = when(storageMock.storeMessageBatchAsync(any()));
        for (int i = 0; i <= MAX_MESSAGE_PERSIST_RETRIES; i++)
            os = os.thenReturn(CompletableFuture.failedFuture(new IOException("message persistence failure")));
        os.thenReturn(CompletableFuture.completedFuture(null));

        Instant timestamp = Instant.now();
        StoredMessageBatch batch = batchOf(createMessage("test-session", Direction.FIRST, 12, timestamp,
                "raw message".getBytes()));
        persistors.getRawPersistor().persist(batch);

        pause(MESSAGE_PERSIST_TIMEOUT * (MAX_MESSAGE_PERSIST_RETRIES + 1));

        ArgumentCaptor<StoredMessageBatch> capture = ArgumentCaptor.forClass(StoredMessageBatch.class);
        verify(persistors, times(MAX_MESSAGE_PERSIST_RETRIES + 1)).processTask(any());
        verify(storageMock, times(MAX_MESSAGE_PERSIST_RETRIES + 1)).storeMessageBatchAsync(capture.capture());

        StoredMessageBatch value = capture.getValue();
        assertNotNull(value, "Captured stored root event");
        assertStoredMessageBatch(batch, value);
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
        StoredMessageBatch[] batch = new StoredMessageBatch[totalMessages];
        for (int i = 0; i < totalMessages; i++)
            batch[i] = batchOf(createMessage("test-session", Direction.FIRST, i, timestamp, "raw message".getBytes()));

        when(storageMock.storeMessageBatchAsync(any()))
                .thenAnswer((ignored) ->  CompletableFuture.runAsync(() -> pause(storeExecutionTime), executor));
        // setup producer thread
        StartableRunnable runnable = StartableRunnable.of(() -> {
            try {
                for (int i = 0; i < totalMessages; i++)
                    persistors.getRawPersistor().persist(batch[i]);
            } catch (Exception e) {
                logger.error("Exception persisting message batch", e);
                throw new RuntimeException(e);
            }
        });

        new Thread(runnable).start();
        runnable.awaitReadiness();
        runnable.start();

        verify(storageMock, after(MESSAGE_PERSIST_TIMEOUT).times(MAX_MESSAGE_QUEUE_TASK_SIZE)).storeMessageBatchAsync(any());
        verify(storageMock, after(totalExecutionTime).times(totalMessages)).storeMessageBatchAsync(any());

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
        StoredMessageBatch[] batch = new StoredMessageBatch[totalMessages];
        for (int i = 0; i < totalMessages; i++)
            batch[i] = batchOf(createMessage("test-session", Direction.FIRST, i, timestamp, content));

        when(storageMock.storeMessageBatchAsync(any()))
                .thenAnswer((ignored) ->  CompletableFuture.runAsync(() ->  pause(storeExecutionTime), executor));

        // setup producer thread
        StartableRunnable runnable = StartableRunnable.of(() -> {
            try {
                for (int i = 0; i < totalMessages; i++)
                    persistors.getRawPersistor().persist(batch[i]);
            } catch (Exception e) {
                logger.error("Exception persisting message batch", e);
                throw new RuntimeException(e);
            }
        });

        new Thread(runnable).start();
        runnable.awaitReadiness();
        runnable.start();

        verify(storageMock, after(MESSAGE_PERSIST_TIMEOUT).times(messageCapacityInQueue)).storeMessageBatchAsync(any());
        verify(storageMock, after(totalExecutionTime).times(totalMessages)).storeMessageBatchAsync(any());

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


    private StoredMessageBatch batchOf(MessageToStore... messages) throws Exception {
        StoredMessageBatch batch = cradleObjectsFactory.createMessageBatch();
        for (MessageToStore message : messages)
            batch.addMessage(message);
        return batch;
    }


    private void assertStoredMessageBatch(StoredMessageBatch expected, StoredMessageBatch actual) {
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getDirection(), actual.getDirection());
        assertEquals(expected.getMessageCount(), actual.getMessageCount());

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
