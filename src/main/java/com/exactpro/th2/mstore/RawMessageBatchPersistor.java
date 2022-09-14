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

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.messages.StoredGroupMessageBatch;
import com.exactpro.th2.taskutils.BlockingScheduledRetryableTaskQueue;
import com.exactpro.th2.taskutils.FutureTracker;
import com.exactpro.th2.taskutils.RetryScheduler;
import com.exactpro.th2.taskutils.ScheduledRetryableTask;
import io.prometheus.client.Histogram;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class RawMessageBatchPersistor implements Runnable, AutoCloseable, Persistor<StoredGroupMessageBatch> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RawMessageBatchPersistor.class);
    private static final String THREAD_NAME_PREFIX = "MessageBatch-persistor-thread-";

    private final BlockingScheduledRetryableTaskQueue<StoredGroupMessageBatch> taskQueue;
    private final int maxTaskRetries;
    private final CradleStorage cradleStorage;
    private final FutureTracker<Void> futures;

    private final MessagePersistorMetrics metrics;
    private final ScheduledExecutorService samplerService;

    private volatile boolean stopped;
    private final Object signal = new Object();

    public RawMessageBatchPersistor(@NotNull Configuration config, @NotNull CradleStorage cradleStorage) {
        this(config, cradleStorage, (r) -> config.getRetryDelayBase() * 1_000_000 * (r + 1));
    }

    public RawMessageBatchPersistor(@NotNull Configuration config, @NotNull CradleStorage cradleStorage, RetryScheduler scheduler) {
        this.maxTaskRetries = config.getMaxRetryCount();
        this.cradleStorage = requireNonNull(cradleStorage, "Cradle storage can't be null");
        this.taskQueue = new BlockingScheduledRetryableTaskQueue<>(config.getMaxTaskCount(), config.getMaxTaskDataSize(), scheduler);
        this.futures = new FutureTracker<>();

        this.metrics = new MessagePersistorMetrics(taskQueue);
        this.samplerService = Executors.newSingleThreadScheduledExecutor();
    }

    public void start() throws InterruptedException {
        this.stopped = false;
        synchronized (signal) {
            new Thread(this, THREAD_NAME_PREFIX + this.hashCode()).start();
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
        samplerService.scheduleWithFixedDelay(
                metrics::takeQueueMeasurements,
                0,
                1,
                TimeUnit.SECONDS
        );
        while (!stopped) {
            try {
                ScheduledRetryableTask<StoredGroupMessageBatch> task = taskQueue.awaitScheduled();
                try {
                    processTask(task);
                } catch (Exception e) {
                    logAndRetry(task, e);
                }
            } catch (InterruptedException ie) {
                LOGGER.debug("Received InterruptedException. aborting");
                break;
            }
        }
    }

    void processTask(ScheduledRetryableTask<StoredGroupMessageBatch> task) {

        final StoredGroupMessageBatch batch = task.getPayload();
        final Histogram.Timer timer = metrics.startMeasuringPersistenceLatency();

        CompletableFuture<Void> result = cradleStorage.storeGroupedMessageBatchAsync(batch, batch.getSessionGroup())
                .thenRun(() -> LOGGER.debug("Stored batch with group '{}'", batch.getSessionGroup()))
                .whenCompleteAsync((unused, ex) ->
                        {
                            timer.observeDuration();
                            if (ex != null)
                                logAndRetry(task, ex);
                            else {
                                taskQueue.complete(task);
                                metrics.updateMessageMeasurements(batch.getMessageCount(), task.getPayloadSize());
                            }
                        }
                );

        futures.track(result);
    }

    @Override
    public void close () {

        LOGGER.info("Waiting for futures completion");
        try {
            stopped = true;
            futures.awaitRemaining();
            LOGGER.info("All waiting futures are completed");
        } catch (Exception ex) {
            LOGGER.error("Cannot await all futures to be finished", ex);
        }
        try {
            samplerService.shutdown();
            samplerService.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
        }
    }

    private void logAndRetry(ScheduledRetryableTask<StoredGroupMessageBatch> task, Throwable e) {

        int retriesDone = task.getRetriesDone() + 1;
        final StoredGroupMessageBatch messageBatch = task.getPayload();

        if (task.getRetriesLeft() > 0) {

            LOGGER.error("Failed to store the message batch for group '{}', {} retries left, rescheduling",
                    messageBatch.getSessionGroup(),
                    task.getRetriesLeft(),
                    e);
            taskQueue.retry(task);
            metrics.registerPersistenceRetry(retriesDone);

        } else {

            taskQueue.complete(task);
            metrics.registerAbortedPersistence();
            LOGGER.error("Failed to store the message batch for group '{}', aborting after {} executions",
                    messageBatch.getSessionGroup(),
                    retriesDone,
                    e);

        }
    }

    @Override
    public void persist(StoredGroupMessageBatch data) {
        metrics.takeQueueMeasurements();
        taskQueue.submit(
                new ScheduledRetryableTask<>(
                        System.nanoTime(),
                        maxTaskRetries,
                        data.getBatchSize(),
                        data)
        );
    }
}