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

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.GuardedBy;

import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.utils.CradleStorageException;

public class SessionBatchHolder {
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final AtomicLong lastSequence = new AtomicLong(Long.MIN_VALUE);

    @GuardedBy("lock")
    private volatile Instant lastReset = Instant.now();

    @GuardedBy("lock")
    private volatile StoredMessageBatch holtBatch = new StoredMessageBatch();

    public long getAndUpdateSequence(long newLastSeq) {
        return lastSequence.getAndAccumulate(newLastSeq, Math::max);
    }

    public boolean add(StoredMessageBatch batch) throws CradleStorageException {
        lock.writeLock().lock();
        try {
            return holtBatch.addBatch(batch);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public boolean isReadyToReset(long maxMillisWithoutReset) {
        lock.readLock().lock();
        try {
            return Math.abs(Duration.between(lastReset, Instant.now()).toMillis()) > maxMillisWithoutReset && !holtBatch.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    public StoredMessageBatch reset() {
        return internalReset(new StoredMessageBatch());
    }

    public StoredMessageBatch resetAndUpdate(StoredMessageBatch batch) {
        Objects.requireNonNull(batch, "'Batch' parameter");
        return internalReset(batch);
    }

    private StoredMessageBatch internalReset(StoredMessageBatch newValue) {
        lock.writeLock().lock();
        try {
            StoredMessageBatch currentBatch = holtBatch;
            holtBatch = newValue;
            lastReset = Instant.now();

            return currentBatch;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
