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

import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.function.Supplier;

public class BatchHolder {

    private final Supplier<GroupedMessageBatchToStore> batchSupplier;

    private volatile Instant lastReset = Instant.now();

    private volatile GroupedMessageBatchToStore holtBatch;
    private final String group;

    public BatchHolder(String group, Supplier<GroupedMessageBatchToStore> batchSupplier) {
        this.batchSupplier = Objects.requireNonNull(batchSupplier, "'Batch supplier' parameter");
        this.group = group;
        this.holtBatch = batchSupplier.get();
    }

    /**
     * Tries to add the batch with messages to the currently holt batch.
     * If the batch can be added because of its size or quantity limitations,
     * the method returns {@code false} without any changes being applied to the currently holt batch.
     * @param batch new batch to add to the currently holt one
     * @return {@code true} if the batch is successfully added to currently holt one. Otherwise, returns {@code false}
     * @throws CradleStorageException if batch doesn't meet requirements for adding to the currently holt one
     */
    public boolean add(GroupedMessageBatchToStore batch) throws CradleStorageException {
        return holtBatch.addBatch(batch);
    }

    /**
     * Checks if the batch is ready to be reset based on max expected time in waiting and its size.
     * @param maxMillisWithoutReset time in milliseconds that must past since the last holder modification
     *                             (creation or last call of {@link #reset} or {@link #resetAndUpdate})
     * @return {@code true} if the holder was modified {@code maxMillisWithoutReset} millis ago and it is not empty
     */
    public boolean isReadyToReset(long maxMillisWithoutReset) {
        return Math.abs(Duration.between(lastReset, Instant.now()).toMillis()) > maxMillisWithoutReset && !holtBatch.isEmpty();
    }

    @NotNull
    public GroupedMessageBatchToStore reset() {
        return internalReset(batchSupplier.get());
    }

    @NotNull
    public GroupedMessageBatchToStore resetAndUpdate(GroupedMessageBatchToStore batch) {
        Objects.requireNonNull(batch, "'Batch' parameter");
        return internalReset(batch);
    }

    private GroupedMessageBatchToStore internalReset(GroupedMessageBatchToStore newValue) {
        GroupedMessageBatchToStore currentBatch = holtBatch;
        holtBatch = newValue;
        lastReset = Instant.now();
        return currentBatch;
    }

    public String getGroup() {
        return group;
    }
}
