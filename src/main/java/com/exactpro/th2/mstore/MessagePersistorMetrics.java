/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.mstore;

import com.exactpro.th2.taskutils.BlockingScheduledRetryableTaskQueue;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

public class MessagePersistorMetrics {
    private static final Gauge GAUGE_QUEUE_TASK_CAPACITY = Gauge
            .build("th2_mstore_persistor_queue_task_capacity", "Message queue size").register();

    private static final Gauge GAUGE_QUEUE_USED_TASK_COUNT = Gauge
            .build("th2_mstore_persistor_queue_task_count", "Number of messages queued for persistence").register();

    private static final Gauge GAUGE_QUEUE_FREE_TASK_COUNT = Gauge
            .build("th2_mstore_persistor_queue_free_tasks", "Number of messages that can be queued").register();

    private static final Gauge GAUGE_QUEUE_MAX_DATA_SIZE = Gauge
            .build("th2_mstore_persistor_queue_max_data_size", "Max data size that can be queued").register();

    private static final Gauge GAUGE_QUEUE_USED_DATA_SIZE = Gauge
            .build("th2_mstore_persistor_queue_used_data_size", "Data size of queued messages").register();

    private static final Gauge GAUGE_QUEUE_FREE_DATA_SIZE = Gauge
            .build("th2_mstore_persistor_queue_free_data_size", "Available data size").register();

    private static final Counter COUNTER_MESSAGES_PERSISTED = Counter
            .build("th2_mstore_persistor_events_persisted", "Number of messages persisted").register();

    private static final Counter COUNTER_MESSAGES_SIZE_PERSISTED = Counter
            .build("th2_mstore_persistor_events_sizes_persisted", "Content size of messages that persisted").register();

    private static final Counter COUNTER_PERSISTENCE_FAILURES = Counter
            .build("th2_mstore_persistor_persistence_failures", "Number of messages persistence failures").register();

    private static final Counter COUNTER_ABORTED_PERSISTENCES = Counter
            .build("th2_mstore_persistor_aborted_persistences", "Number of aborted messages persistences").register();

    private static final Counter COUNTER_1ST_RETRIES = Counter
            .build("th2_mstore_persistor_1st_retries", "Number of primary persistence retries").register();

    private static final Counter COUNTER_2ND_RETRIES = Counter
            .build("th2_mstore_persistor_2nd_retries", "Number of secondary persistence retries").register();

    private static final Counter COUNTER_FURTHER_RETRIES = Counter
            .build("th2_mstore_persistor_further_retries", "Number of further persistence retries").register();

    private static final Histogram HISTOGRAM_PERSISTENCE_LATENCY = Histogram
            .build("th2_mstore_persistor_persistence_latency", "Message persistence latency")
            .buckets(0.010, 0.020, 0.050, 0.100, 0.200, 0.300, 0.400, 0.500, 1.000, 1.500, 2.000, 2.500, 3.000, 4.000, 5.000, 10.000)
            .register();

    private final BlockingScheduledRetryableTaskQueue taskQueue;

    public MessagePersistorMetrics(BlockingScheduledRetryableTaskQueue taskQueue) {
        this.taskQueue = taskQueue;

    }

    public void takeQueueMeasurements() {

        int usedTasks = taskQueue.getTaskCount();
        int freeTasks = taskQueue.getMaxTaskCount() - usedTasks;

        GAUGE_QUEUE_TASK_CAPACITY.set(taskQueue.getMaxTaskCount());
        GAUGE_QUEUE_USED_TASK_COUNT.set(usedTasks);
        GAUGE_QUEUE_FREE_TASK_COUNT.set(freeTasks);

        long maxDataSize = taskQueue.getMaxDataSize();
        long usedDataSize = taskQueue.getUsedDataSize();
        long freeDataSize = maxDataSize - usedDataSize;

        GAUGE_QUEUE_MAX_DATA_SIZE.set(maxDataSize);
        GAUGE_QUEUE_USED_DATA_SIZE.set(usedDataSize);
        GAUGE_QUEUE_FREE_DATA_SIZE.set(freeDataSize);
    }


    public void updateMessageMeasurements(int messages, long messageSizes) {

        COUNTER_MESSAGES_PERSISTED.inc(messages);
        COUNTER_MESSAGES_SIZE_PERSISTED.inc(messageSizes);
    }


    public Histogram.Timer startMeasuringPersistenceLatency() {

        return HISTOGRAM_PERSISTENCE_LATENCY.startTimer();
    }


    public void registerPersistenceFailure() {
        COUNTER_PERSISTENCE_FAILURES.inc();
    }


    public void registerAbortedPersistence() {
        COUNTER_ABORTED_PERSISTENCES.inc();
    }


    public void registerPersistenceRetry(int retryNumber) {
        switch (retryNumber) {
            case 1:
                COUNTER_1ST_RETRIES.inc();
                break;
            case 2:
                COUNTER_2ND_RETRIES.inc();
                break;
            default:
                if (retryNumber < 1)
                    throw new IllegalArgumentException("Invalid value : " + retryNumber);
                else
                    COUNTER_FURTHER_RETRIES.inc();
        }
    }
}