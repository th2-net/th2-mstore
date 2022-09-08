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
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.th2.taskutils.BlockingScheduledRetryableTaskQueue;
import com.exactpro.th2.taskutils.FutureTracker;
import com.exactpro.th2.taskutils.RetryScheduler;
import com.exactpro.th2.taskutils.ScheduledRetryableTask;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class MessageBatchPersistors implements Runnable, AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBatchPersistors.class);
    private static final String THREAD_NAME_PREFIX = "MessageBatch-persistor-thread-";

    private final Persistor<StoredMessageBatch> rawPersistor;
    private final Persistor<StoredMessageBatch> parsedPersistor;

    private final BlockingScheduledRetryableTaskQueue<PersistenceTask> taskQueue;
    private final int maxTaskRetries;
    private CradleStorage cradleStorage;
    private final FutureTracker<Void> futures;

    private volatile boolean stopped;
    private final Object signal = new Object();

    public MessageBatchPersistors(@NotNull Configuration config, @NotNull CradleStorage cradleStorage) {
        this(config, cradleStorage, (r) -> config.getRetryDelayBase() * 1_000_000 * (r + 1));
    }

    public MessageBatchPersistors(@NotNull Configuration config, @NotNull CradleStorage cradleStorage, RetryScheduler scheduler) {
        this.maxTaskRetries = config.getMaxRetryCount();
        this.cradleStorage = requireNonNull(cradleStorage, "Cradle storage can't be null");
        this.taskQueue = new BlockingScheduledRetryableTaskQueue<>(config.getMaxTaskCount(), config.getMaxTaskDataSize(), scheduler);
        this.futures = new FutureTracker<>();

        this.rawPersistor = new RawPersistor();
        this.parsedPersistor = new ParsedPersistor();
    }

    public Persistor<StoredMessageBatch> getRawPersistor() {
        return rawPersistor;
    }

    public Persistor<StoredMessageBatch> getParsedPersistor() {
        return parsedPersistor;
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
        while (!stopped) {
            try {
                ScheduledRetryableTask<PersistenceTask> task = taskQueue.awaitScheduled();
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

    private void processTask(ScheduledRetryableTask<PersistenceTask> task) {

        final PersistenceTask persistenceTask = task.getPayload();
        final StoredMessageBatch batch = persistenceTask.messageBatch;

        CompletableFuture<Void> result = persistenceTask.storeFunction.apply(batch)
                .thenRun(() -> LOGGER.debug("Stored batch id '{}'", batch.getId()))
                .whenCompleteAsync((unused, ex) ->
                        {
                            if (ex != null)
                                logAndRetry(task, ex);
                            else {
                                taskQueue.complete(task);
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
    }

    private void logAndRetry(ScheduledRetryableTask<PersistenceTask> task, Throwable e) {

        int retriesDone = task.getRetriesDone() + 1;
        final StoredMessageBatch messageBatch =task.getPayload().messageBatch;

        if (task.getRetriesLeft() > 0) {

            LOGGER.error("Failed to store the message batch id '{}', {} retries left, rescheduling",
                    messageBatch.getId(),
                    task.getRetriesLeft(),
                    e);
            taskQueue.retry(task);

        } else {

            taskQueue.complete(task);
            LOGGER.error("Failed to store the message batch id '{}', aborting after {} executions",
                    messageBatch.getId(),
                    retriesDone,
                    e);

        }
    }

    private CompletableFuture<Void> storeRaw(StoredMessageBatch batch) {
        return cradleStorage.storeMessageBatchAsync(batch);
    }

    private CompletableFuture<Void> storeParsed(StoredMessageBatch batch) {
        return cradleStorage.storeProcessedMessageBatchAsync(batch);
    }

    static class PersistenceTask {
        final StoredMessageBatch messageBatch;
        final Function<StoredMessageBatch, CompletableFuture<Void>> storeFunction;

        PersistenceTask(Function<StoredMessageBatch, CompletableFuture<Void>> storeFunction,
                        StoredMessageBatch messageBatch) {
            this.storeFunction = storeFunction;
            this.messageBatch = messageBatch;
       }
    }

    class RawPersistor implements Persistor<StoredMessageBatch> {
        @Override
        public void persist(StoredMessageBatch data) {
            taskQueue.submit(
                    new ScheduledRetryableTask<>(
                            System.nanoTime(),
                            maxTaskRetries,
                            data.getBatchSize(),
                            new PersistenceTask(MessageBatchPersistors.this::storeRaw, data))
            );
        }
    }

    class ParsedPersistor implements Persistor<StoredMessageBatch> {
        @Override
        public void persist(StoredMessageBatch data) {
            taskQueue.submit(
                    new ScheduledRetryableTask<>(
                            System.nanoTime(),
                            maxTaskRetries,
                            data.getBatchSize(),
                            new PersistenceTask(MessageBatchPersistors.this::storeParsed, data))
            );
        }
    }
}
