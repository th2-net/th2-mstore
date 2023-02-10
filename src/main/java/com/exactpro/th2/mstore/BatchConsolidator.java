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

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback.Confirmation;

public class BatchConsolidator {

    private final Supplier<GroupedMessageBatchToStore> batchSupplier;

    private volatile long lastReset = System.nanoTime();
    private volatile ConsolidatedBatch data;

    public BatchConsolidator(Supplier<GroupedMessageBatchToStore> batchSupplier) {
        this.batchSupplier = Objects.requireNonNull(batchSupplier, "'Batch supplier' parameter");
        this.data = new ConsolidatedBatch(batchSupplier.get(), null);
    }

    public boolean add(GroupedMessageBatchToStore batch, Confirmation confirmation) throws CradleStorageException {
        if (data.confirmations.size() > 0)
            return false;
        return false;
//        if (!data.batch.addBatch(batch))
//            return false;
//        data.confirmations.add(confirmation);
//        return true;
    }

    public long ageInMillis() {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastReset);
    }

    @NotNull
    public ConsolidatedBatch reset() {
        return internalReset(batchSupplier.get(), null);
    }

    @NotNull
    public ConsolidatedBatch resetAndUpdate(GroupedMessageBatchToStore batch, Confirmation confirmation) {
        Objects.requireNonNull(batch, "batch must not be null");
        Objects.requireNonNull(confirmation, "confirmation must not be null");
        return internalReset(batch, confirmation);
    }

    public boolean isEmpty() {
        return data.batch.isEmpty();
    }

    private ConsolidatedBatch internalReset(GroupedMessageBatchToStore newValue, Confirmation confirmation) {
        ConsolidatedBatch currentData = data;
        data = new ConsolidatedBatch(newValue, confirmation);
        lastReset = System.nanoTime();
        return currentData;
    }
}
