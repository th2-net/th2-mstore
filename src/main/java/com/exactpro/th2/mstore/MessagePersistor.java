/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.errors.BookNotFoundException;
import com.exactpro.cradle.errors.PageNotFoundException;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.th2.common.utils.ExecutorServiceUtilsKt;
import com.exactpro.th2.taskutils.BlockingScheduledRetryableTaskQueue;
import com.exactpro.th2.taskutils.FutureTracker;
import com.exactpro.th2.taskutils.RetryScheduler;
import com.exactpro.th2.taskutils.ScheduledRetryableTask;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.prometheus.client.Histogram;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class MessagePersistor implements Runnable, AutoCloseable, Persistor<GroupedMessageBatchToStore> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessagePersistor.class);
    private static final String THREAD_NAME_PREFIX = "MessageBatch-persistor-thread-";

    private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder().setNameFormat("mstore-message-persistor-%d").build();

    private final BlockingScheduledRetryableTaskQueue<PersistenceTask<GroupedMessageBatchToStore>> taskQueue;
    private final int maxTaskRetries;
    private final long terminationTimeout;
    private final CradleStorage cradleStorage;
    private final ErrorCollector errorCollector;
    private final FutureTracker<Void> futures;

    private final MessagePersistorMetrics<PersistenceTask<GroupedMessageBatchToStore>> metrics;
    private final ScheduledExecutorService executor;

    private volatile boolean stopped;
    private final Object signal = new Object();
    private final AtomicReference<Thread> persistor = new AtomicReference<>();

    public MessagePersistor(@NotNull ErrorCollector errorCollector, @NotNull CradleStorage cradleStorage, @NotNull Configuration config) {
        this(errorCollector, cradleStorage, (r) -> config.getRetryDelayBase() * 1_000_000 * (r + 1), config);
    }

    public MessagePersistor(@NotNull ErrorCollector errorCollector, @NotNull CradleStorage cradleStorage, RetryScheduler scheduler, @NotNull Configuration config) {
        this.maxTaskRetries = config.getMaxRetryCount();
        this.cradleStorage = requireNonNull(cradleStorage, "Cradle storage can't be null");
        this.errorCollector = requireNonNull(errorCollector, "Error collector can't be null");
        this.taskQueue = new BlockingScheduledRetryableTaskQueue<>(config.getMaxTaskCount(), config.getMaxTaskDataSize(), scheduler);
        this.terminationTimeout = config.getPersisotrTerminationTimeout();
        this.futures = new FutureTracker<>();

        this.metrics = new MessagePersistorMetrics<>(taskQueue);
        this.executor = Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), THREAD_FACTORY); // FIXME: make thread count configurable
    }

    public void start() throws InterruptedException {
        if (persistor.get() != null) {
            throw new IllegalStateException("Message persistor has been already started once");
        }
        this.stopped = false;
        synchronized (signal) {
            Thread thread = new Thread(this, THREAD_NAME_PREFIX + this.hashCode());
            if (!persistor.compareAndSet(null, thread)) {
                throw new IllegalStateException("Message persistor is already started");
            }
            thread.start();
            signal.wait();
        }
    }

    @Override
    public void run() {
        synchronized (signal) {
            signal.notifyAll();
        }

        LOGGER.info("Message batch persistor started. Maximum data size for tasks = {}, maximum number of tasks = {}",
                taskQueue.getMaxDataSize(), taskQueue.getMaxTaskCount());
        executor.scheduleWithFixedDelay(
                metrics::takeQueueMeasurements,
                0,
                1,
                TimeUnit.SECONDS
        );
        while (!stopped) {
            try {
                ScheduledRetryableTask<PersistenceTask<GroupedMessageBatchToStore>> task = taskQueue.awaitScheduled();
                try {
                    processTask(task);
                } catch (Exception e) {
                    resolveTaskError(task, e);
                }
            } catch (InterruptedException ie) {
                LOGGER.debug("Received InterruptedException. aborting");
                break;
            }
        }
    }

    void processTask(ScheduledRetryableTask<PersistenceTask<GroupedMessageBatchToStore>> task) throws Exception {

        final GroupedMessageBatchToStore batch = task.getPayload().data;
        final Histogram.Timer timer = metrics.startMeasuringPersistenceLatency();

        CompletableFuture<Void> result = cradleStorage.storeGroupedMessageBatchAsync(batch)
                .thenRun(() -> LOGGER.trace("Stored batch with group '{}'", batch.getGroup()))
                .whenCompleteAsync((unused, ex) ->
                        {
                            timer.observeDuration();
                            if (ex != null) {
                                resolveTaskError(task, ex);
                            } else {
                                taskQueue.complete(task);
                                metrics.updateMessageMeasurements(batch.getMessageCount(), task.getPayloadSize());
                                task.getPayload().complete();
                            }
                        }
                        , executor);

        futures.track(result);
    }

    private void resolveTaskError(ScheduledRetryableTask<PersistenceTask<GroupedMessageBatchToStore>> task, Throwable e) {
        if (e instanceof BookNotFoundException || e instanceof PageNotFoundException) {
            // If following exceptions were thrown there's no point in retrying
            logAndFail(task,
                    "Can't retry after an exception",
                    String.format("Can't retry after %s exception for group '%s' in book '%s'",
                            e.getClass(), task.getPayload().data.getGroup(), task.getPayload().data.getBookId().getName()),
                    e
            );
        } else {
            logAndRetry(task, e);
        }
    }

    @Override
    public void close () {

        LOGGER.info("Waiting for futures completion");
        try {
            stopped = true;
            futures.awaitRemaining();
            LOGGER.info("All waiting futures are completed");

            Thread thread = persistor.get();
            if (thread != null && thread.isAlive()) {
                thread.interrupt();
                thread.join(terminationTimeout);
                if (thread.isAlive()) {
                    LOGGER.warn("Persistor thread hasn't been stopped");
                } else {
                    LOGGER.info("Persistor thread has been stopped");
                }
            }
        } catch (Exception ex) {
            errorCollector.collect(LOGGER, "Cannot await all futures to be finished", ex);
        }
        ExecutorServiceUtilsKt.shutdownGracefully(executor, 1, TimeUnit.MINUTES);
    }

    private void logAndRetry(ScheduledRetryableTask<PersistenceTask<GroupedMessageBatchToStore>> task, Throwable e) {

        int retriesDone = task.getRetriesDone() + 1;
        GroupedMessageBatchToStore messageBatch = task.getPayload().data;

        if (task.getRetriesLeft() > 0) {
            errorCollector.collect("Failed to store the message batch for group '" + messageBatch.getGroup() + "' retries left, rescheduling");
            LOGGER.error("Failed to store the message batch for group '{}', {} retries left, rescheduling",
                    messageBatch.getGroup(),
                    task.getRetriesLeft(),
                    e);
            taskQueue.retry(task);
            metrics.registerPersistenceRetry(retriesDone);

        } else {
            logAndFail(task,
                    "Failed to store the message batch for a group '" + messageBatch.getGroup() + "', aborting after executions",
                    String.format("Failed to store the message batch for group '%s', aborting after %d executions",
                            messageBatch.getGroup(),
                            retriesDone),
                    e);
        }
    }

    private void logAndFail(
            ScheduledRetryableTask<PersistenceTask<GroupedMessageBatchToStore>> task,
            String errorKey,
            String logMessage,
            Throwable cause
    ) {
        taskQueue.complete(task);
        metrics.registerAbortedPersistence();
        errorCollector.collect(errorKey);
        LOGGER.error(logMessage, cause);
        task.getPayload().fail();
    }

    @Override
    public void persist(GroupedMessageBatchToStore data, Callback<GroupedMessageBatchToStore> callback) {
        metrics.takeQueueMeasurements();
        taskQueue.submit(
                new ScheduledRetryableTask<>(
                        System.nanoTime(),
                        maxTaskRetries,
                        data.getBatchSize(),
                        new PersistenceTask<>(data, callback))
        );
    }


    private static class PersistenceTask<V> {
        final V data;
        final Callback<V> callback;

        PersistenceTask(V data, Callback<V> callback) {
            this.data = data;
            this.callback = callback;
        }

        void complete() {
            if (callback != null) {
                callback.onSuccess(data);
            }
        }

        void fail() {
            if (callback != null) {
                callback.onFail(data);
            }
        }
    }
}