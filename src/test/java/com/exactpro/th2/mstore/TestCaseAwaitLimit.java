/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.mstore.cfg.MessageStoreConfiguration;
import com.google.protobuf.GeneratedMessageV3;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
abstract class TestCaseAwaitLimit<T extends GeneratedMessageV3, M extends GeneratedMessageV3> extends AbstractStorageTest<T, M> {

    protected TestCaseAwaitLimit(CradleStoreFunction storeFunction) {
        super(storeFunction);
    }

    @SuppressWarnings("unchecked")
    protected CompletableFuture<Void> createCompletableFuture() {
        return spy(CompletableFuture.class);
    }


    protected abstract AbstractMessageStore<T, M> createStore(CradleManager cradleManagerMock, MessageRouter<T> routerMock, MessageStoreConfiguration configuration);

    protected abstract M createMessage(String session, Direction direction, long sequence);

    protected abstract long extractSize(M message);

    protected abstract T createDelivery(List<M> messages);

    @Override
    @NotNull
    protected MessageStoreConfiguration createConfiguration() {
        MessageStoreConfiguration configuration = new MessageStoreConfiguration();
        configuration.setAsyncStoreLimit(1);
        configuration.setLimitExceededTimeout(DRAIN_TIMEOUT / 10);
        configuration.setDrainInterval(DRAIN_TIMEOUT / 10);
        return configuration;
    }

    @Test
    void storedNormallyWhenLimitIsNotReached() throws InterruptedException {
        M first = createMessage("testA", Direction.FIRST, 1);
        messageStore.handle(deliveryOf(first));

        storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT).times(1)), any());
        verify(cradleObjectsFactory, times(3)).createMessageBatch();
        completableFuture.complete(null);
    }

    @Test
    void awaitsMessagesAreStored() throws InterruptedException {
        messageStore.handle(deliveryOf(createMessage("testA", Direction.FIRST, 1)));
        storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT).times(1)), any());

        Assertions.assertThrows(TimeoutException.class, () -> {
            runAsync(() -> {
                try {
                    M first = createMessage("testA", Direction.FIRST, 2);
                    messageStore.handle(deliveryOf(first));
                } catch (Exception ex) {
                    ExceptionUtils.rethrow(ex);
                }
            }).get(DRAIN_TIMEOUT, TimeUnit.MILLISECONDS);
        });
        completableFuture.complete(null);
    }

    @Test
    void stopWaitingWhenMessagesAraStored() throws InterruptedException {
        CompletableFuture<Void> completed = CompletableFuture.completedFuture(null);
        //noinspection unchecked
        when(storageMock.storeProcessedMessageBatchAsync(any(StoredMessageBatch.class))).thenReturn(completableFuture, completed);
        //noinspection unchecked
        when(storageMock.storeMessageBatchAsync(any(StoredMessageBatch.class))).thenReturn(completableFuture, completed);

        messageStore.handle(deliveryOf(createMessage("testA", Direction.FIRST, 1)));
        storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT).times(1)), any());

        Assertions.assertDoesNotThrow(() -> {
            allOf(
                    runAsync(() -> {
                        try {
                            Thread.sleep(100);
                            completableFuture.complete(null);
                        } catch (InterruptedException ex) {
                            ExceptionUtils.rethrow(ex);
                        }
                    }),
                    runAsync(() -> {
                        try {
                            M first = createMessage("testA", Direction.SECOND, 2);
                            messageStore.handle(deliveryOf(first));
                            storeFunction.store(verify(storageMock, timeout(DRAIN_TIMEOUT).times(1)), any());
                        } catch (Exception ex) {
                            ExceptionUtils.rethrow(ex);
                        }
                    })
            ).get(DRAIN_TIMEOUT, TimeUnit.MILLISECONDS);
        });
    }
}
