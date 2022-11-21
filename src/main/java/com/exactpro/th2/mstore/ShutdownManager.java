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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.exactpro.th2.common.metrics.CommonMetrics.LIVENESS_MONITOR;
import static com.exactpro.th2.common.metrics.CommonMetrics.READINESS_MONITOR;

public final class ShutdownManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownManager.class);
    private static final String SHUTDOWN_HOOK_THREAD_NAME = "th2_shutdown_hook";

    private final Deque<AutoCloseable> resources = new ConcurrentLinkedDeque<>();
    private volatile boolean shuttingDown = false;
    private final Lock shutdownLock;
    private final Condition shutdown;

    public ShutdownManager() {
        shutdownLock = new ReentrantLock();
        shutdown = shutdownLock.newCondition();
    }


    public void registerResource(AutoCloseable resource) {
        resources.add(resource);
    }


    public void register() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutdown started");
            READINESS_MONITOR.disable();

            closeResources();
            signalShutdown();

            LIVENESS_MONITOR.disable();
            LOGGER.info("Shutdown ended");

        }, SHUTDOWN_HOOK_THREAD_NAME));
    }


    public void awaitShutdown() {
        shutdownLock.lock();
        try {
            while (!shuttingDown)
                try {
                    shutdown.await();
                } catch (InterruptedException e) {
                }
        } finally {
            shutdownLock.unlock();
        }
    }


    public synchronized void closeResources() {

        Iterator<AutoCloseable> iterator = resources.descendingIterator();
        while (iterator.hasNext()) {
            AutoCloseable resource = iterator.next();
            try {
                resource.close();
                iterator.remove();
            } catch (Exception e) {
                LOGGER.error("Exception closing resource {}", resource, e);
            }
        }
    }


    private void signalShutdown() {
        shutdownLock.lock();
        try {
            shuttingDown = true;
            shutdown.signalAll();
        } finally {
            shutdownLock.unlock();
        }
    }
}