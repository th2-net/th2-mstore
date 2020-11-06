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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.concurrent.GuardedBy;

import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.utils.CradleStorageException;

public class SessionBatchHolder {
    private final Lock lock = new ReentrantLock();

    private final AtomicLong lastSequence = new AtomicLong(Long.MIN_VALUE);

    @GuardedBy("lock")
    private volatile StoredMessageBatch holtBatch = new StoredMessageBatch();

    public long getAndUpdateSequence(long newLastSeq) {
        return lastSequence.getAndAccumulate(newLastSeq, Math::max);
    }

    public boolean add(StoredMessageBatch batch) throws CradleStorageException {
        lock.lock();
        try {
            return holtBatch.addBatch(batch);
        } finally {
            lock.unlock();
        }
    }

    public StoredMessageBatch reset() {
        lock.lock();
        try {
            StoredMessageBatch currentBatch = holtBatch;
            holtBatch = new StoredMessageBatch();
            return currentBatch;
        } finally {
            lock.unlock();
        }
    }
}
