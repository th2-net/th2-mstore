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

import java.util.concurrent.ExecutorService;

public class ManualDrainTrigger {
    private final ExecutorService executor;
    private final int threshold;
    private volatile long counter;
    private volatile boolean drainTriggered;

    ManualDrainTrigger(ExecutorService executor, int threshold) {
        this.executor = executor;
        this.threshold = threshold;
    }

    public long getCounter() {
        return counter;
    }

    public boolean isDrainTriggered() {
        return drainTriggered;
    }

    synchronized void registerMessage() {
        counter++;
    }

    synchronized void unregisterMessages(int count) {
        counter -= count;
    }

    synchronized void completeDraining() {
        drainTriggered = false;
    }

    synchronized void runConditionally(Runnable drainer) {
        if (threshold > 0) {
            if (!drainTriggered && (counter > threshold)) {
                drainTriggered = true;
                executor.submit(drainer);
            }
        }
    }
}