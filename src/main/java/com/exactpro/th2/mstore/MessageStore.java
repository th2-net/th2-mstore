/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import static com.exactpro.th2.common.metrics.CommonMetrics.setLiveness;
import static com.exactpro.th2.common.metrics.CommonMetrics.setReadiness;

import java.net.InetAddress;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.schema.factory.AbstractCommonFactory;
import com.exactpro.th2.common.schema.factory.CommonFactory;
import com.exactpro.th2.store.common.utils.CradleUtil;

public class MessageStore {

    private static final Logger logger = LoggerFactory.getLogger(MessageStore.class);

    private final MessageBatchStore parsedStore;
    private final RawMessageBatchStore rawStore;
    private final CradleManager cradleManager;

    public MessageStore(AbstractCommonFactory factory) throws CradleStorageException {
        this.cradleManager = CradleUtil.createCradleManager(factory.getCradleConfiguration());

        parsedStore = new MessageBatchStore(factory.getMessageRouterParsedBatch(), cradleManager);
        rawStore = new RawMessageBatchStore(factory.getMessageRouterRawBatch(), cradleManager);
    }

    public void start() {
        try {
            String cradleInstanceName = InetAddress.getLocalHost().getHostName();
            cradleManager.init(cradleInstanceName);
            logger.info("Cradle manager init successfully with {} instance name", cradleInstanceName);
        } catch (Exception e) {
            throw new IllegalStateException("Can not create cradle manager", e);
        }

        try {
            parsedStore.start();
        } catch (Exception e) {
            throw new IllegalStateException("Can not start storage for parsed messages", e);
        }

        try {
            rawStore.start();
            logger.info("Message store start successfully");
        } catch (Exception e) {
            try {
                parsedStore.dispose();
            } finally {
                throw new IllegalStateException("Can not start storage for raw messages", e);
            }

        }
    }

    public void dispose() {
        try {
            parsedStore.dispose();
        } catch (Exception e) {
            logger.error("Cannot dispose storage for parsed messages", e);
        }

        try {
            rawStore.dispose();
        } catch (Exception e) {
            logger.error("Cannot dispose storage for raw messages", e);
        }
        logger.info("Storage stopped");
    }

    public static void main(String[] args) {
        Deque<AutoCloseable> resources = new ConcurrentLinkedDeque<>();
        ReentrantLock lock = new ReentrantLock();
        Condition condition = lock.newCondition();

        configureShutdownHook(resources, lock, condition);
        try {
            setLiveness(true);
            CommonFactory factory = CommonFactory.createFromArguments(args);
            resources.add(factory);
            CradleManager cradleManager = factory.getCradleManager();
            resources.add(cradleManager::dispose);
            MessageStore store = new MessageStore(factory);
            resources.add(store::dispose);
            store.start();
            setReadiness(true);
            logger.info("message store started");
            awaitShutdown(lock, condition);
        } catch (InterruptedException e) {
            logger.info("The main thread interupted", e);
        } catch (Exception e) {
            logger.error("Fatal error: {}", e.getMessage(), e);
            System.exit(1);
        }
    }

    private static void awaitShutdown(ReentrantLock lock, Condition condition) throws InterruptedException {
        try {
            lock.lock();
            logger.info("Wait shutdown");
            condition.await();
            logger.info("App shutdowned");
        } finally {
            lock.unlock();
        }
    }

    private static void configureShutdownHook(Deque<AutoCloseable> resources, ReentrantLock lock, Condition condition) {
        Runtime.getRuntime().addShutdownHook(new Thread("Shutdown hook") {
            @Override
            public void run() {
                logger.info("Shutdown start");
                setReadiness(false);
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
                        logger.error(e.getMessage(), e);
                    }
                });
                setLiveness(false);
                logger.info("Shutdown end");
            }
        });
    }
}

