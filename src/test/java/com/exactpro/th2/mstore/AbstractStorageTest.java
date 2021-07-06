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

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleObjectsFactory;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.testevents.StoredTestEventBatch;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.mstore.cfg.MessageStoreConfiguration;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Timestamp;
import com.google.protobuf.TimestampOrBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

abstract class AbstractStorageTest<T extends GeneratedMessageV3, M extends GeneratedMessageV3> {
    protected static final int DRAIN_TIMEOUT = 1000;
    protected static final long TEST_MESSAGE_BATCH_SIZE = 1024;
    protected static final long TEST_EVENT_BATCH_SIZE = StoredTestEventBatch.DEFAULT_MAX_BATCH_SIZE;

    protected final CradleManager cradleManagerMock = mock(CradleManager.class);

    protected final CradleStorage storageMock = mock(CradleStorage.class);

    @SuppressWarnings("unchecked")
    protected final MessageRouter<T> routerMock = (MessageRouter<T>) mock(MessageRouter.class);

    protected final CradleStoreFunction storeFunction;

    protected final CompletableFuture<Void> completableFuture = createCompletableFuture();

    protected AbstractMessageStore<T, M> messageStore;

    protected CradleObjectsFactory cradleObjectsFactory;

    protected AbstractStorageTest(CradleStoreFunction storeFunction) {
        this.storeFunction = storeFunction;
    }

    @BeforeEach
    void setUp() {
        cradleObjectsFactory = spy(new CradleObjectsFactory(TEST_MESSAGE_BATCH_SIZE, TEST_EVENT_BATCH_SIZE));

        when(storageMock.getObjectsFactory()).thenReturn(cradleObjectsFactory);
        when(storageMock.storeProcessedMessageBatchAsync(any(StoredMessageBatch.class))).thenReturn(completableFuture);
        when(storageMock.storeMessageBatchAsync(any(StoredMessageBatch.class))).thenReturn(completableFuture);

        when(cradleManagerMock.getStorage()).thenReturn(storageMock);
        when(routerMock.subscribeAll(any(), any())).thenReturn(mock(SubscriberMonitor.class));
        MessageStoreConfiguration configuration = createConfiguration();
        messageStore = spy(createStore(cradleManagerMock, routerMock, configuration));
        messageStore.start();
    }

    @NotNull
    protected abstract MessageStoreConfiguration createConfiguration();

    protected abstract CompletableFuture<Void> createCompletableFuture();

    @AfterEach
    void tearDown() {
        messageStore.dispose();
    }

    protected abstract AbstractMessageStore<T, M> createStore(CradleManager cradleManagerMock, MessageRouter<T> routerMock, MessageStoreConfiguration configuration);

    protected abstract M createMessage(String session, Direction direction, long sequence);

    protected abstract long extractSize(M message);

    protected abstract T createDelivery(List<M> messages);

    protected abstract Timestamp extractTimestamp(M message);

    @SafeVarargs
    @SuppressWarnings("varargs")
    protected final T deliveryOf(M... messages) {
        return createDelivery(List.of(messages));
    }

    @NotNull
    protected final MessageID createMessageId(String session, Direction direction, long sequence) {
        return MessageID.newBuilder()
                .setDirection(direction)
                .setSequence(sequence)
                .setConnectionId(ConnectionID.newBuilder().setSessionAlias(session).build())
                .build();
    }

    protected final Timestamp createTimestamp() {
        Instant now = Instant.now();
        return Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();
    }

    protected static Instant from(TimestampOrBuilder timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }

    protected interface CradleStoreFunction {
        CompletableFuture<Void> store(CradleStorage storage, StoredMessageBatch batch);
    }
}
