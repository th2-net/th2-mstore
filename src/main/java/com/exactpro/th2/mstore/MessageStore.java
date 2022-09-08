/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.th2.common.schema.factory.AbstractCommonFactory;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.exactpro.th2.common.metrics.CommonMetrics.LIVENESS_MONITOR;
import static com.exactpro.th2.common.metrics.CommonMetrics.READINESS_MONITOR;

public class MessageStore implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageStore.class);

    private final MessageBatchPersistors persistor;
    private final MessageBatchStore parsedStore;
    private final RawMessageBatchStore rawStore;
    private final CradleManager cradleManager;

    public MessageStore(AbstractCommonFactory factory, Deque<AutoCloseable> resources) {
        cradleManager = factory.getCradleManager();
        Configuration configuration = factory.getCustomConfiguration(Configuration.class);
        CradleStorage storage = cradleManager.getStorage();

        persistor = new MessageBatchPersistors(configuration, storage);
        resources.add(persistor);

        rawStore = new RawMessageBatchStore(factory.getMessageRouterRawBatch(),
                                            storage,
                                            persistor.getRawPersistor(),
                                            configuration);
        resources.add(rawStore);

        parsedStore = new MessageBatchStore(factory.getMessageRouterParsedBatch(),
                                            storage,
                                            persistor.getParsedPersistor(),
                                            configuration);
        resources.add(parsedStore);
    }

    public void start() throws Exception {
        persistor.start();
        parsedStore.start();
        rawStore.start();
    }

    public void close () {

        try {
            cradleManager.dispose();
        } catch (Exception e) {
            LOGGER.error("Cannot dispose cradle manager", e);
        }
        LOGGER.info("Storage stopped");
    }

    public static void main(String[] args) {
        Deque<AutoCloseable> resources = new ConcurrentLinkedDeque<>();
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();

        configureShutdownHook(resources, lock, condition);
        try {
            LIVENESS_MONITOR.enable();

            CommonFactory factory = CommonFactory.createFromArguments(args);
            resources.add(factory);

            MessageStore store = new MessageStore(factory, resources);
            resources.add(store);
            store.start();

            READINESS_MONITOR.enable();
            LOGGER.info("message store started");

            awaitShutdown(lock, condition);
        } catch (InterruptedException e) {
            LOGGER.info("The main thread interrupted", e);
        } catch (Exception e) {
            LOGGER.error("Fatal error: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    private static void awaitShutdown(ReentrantLock lock, Condition condition) throws InterruptedException {
        try {
            lock.lock();
            LOGGER.info("Waiting for shutdown");
            condition.await();
            LOGGER.info("Message Store shutdown");
        } finally {
            lock.unlock();
        }
    }

    private static void configureShutdownHook(Deque<AutoCloseable> resources, ReentrantLock lock, Condition condition) {
        Runtime.getRuntime().addShutdownHook(new Thread("Shutdown hook") {
            @Override
            public void run() {
                LOGGER.info("Shutdown start");
                READINESS_MONITOR.disable();
                try {
                    lock.lock();
                    condition.signalAll();
                } finally {
                    lock.unlock();
                }

                resources.descendingIterator().forEachRemaining(resource -> {
                    try {
                        resource.close();
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                    }
                });
                LIVENESS_MONITOR.disable();
                LOGGER.info("Shutdown end");
            }
        });
    }
}

