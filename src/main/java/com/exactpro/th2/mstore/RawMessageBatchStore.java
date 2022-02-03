/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.mstore.cfg.MessageStoreConfiguration;

import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static com.exactpro.th2.common.util.StorageUtils.toInstant;

public class RawMessageBatchStore extends AbstractMessageStore<RawMessageBatch, RawMessage> {
    private static final String[] ATTRIBUTES = Stream.of(QueueAttribute.SUBSCRIBE, QueueAttribute.RAW)
            .map(QueueAttribute::toString)
            .toArray(String[]::new);

    public RawMessageBatchStore(
            MessageRouter<RawMessageBatch> router,
            @NotNull CradleManager cradleManager,
            @NotNull MessageStoreConfiguration configuration
    ) {
        super(router, cradleManager, configuration);
    }

    @Override
    protected MessageToStore convert(RawMessage originalMessage) {
        return ProtoUtil.toCradleMessage(originalMessage);
    }

    @Override
    protected CompletableFuture<Void> store(StoredMessageBatch storedMessageBatch) {
        return cradleStorage.storeMessageBatchAsync(storedMessageBatch);
    }

    @Override
    protected long extractSequence(RawMessage message) {
        return message.getMetadata().getId().getSequence();
    }

    @Override
    protected SessionKey createSessionKey(RawMessage message) {
        MessageID messageID = message.getMetadata().getId();
        return new SessionKey(messageID.getConnectionId().getSessionAlias(), toCradleDirection(messageID.getDirection()));
    }

    @Override
    protected Instant extractTimestamp(RawMessage message) {
        return toInstant(message.getMetadata().getTimestamp());
    }

    @Override
    protected String[] getAttributes() {
        return ATTRIBUTES;
    }

    @Override
    protected List<RawMessage> getMessages(RawMessageBatch delivery) {
        return delivery.getMessagesList();
    }
}
