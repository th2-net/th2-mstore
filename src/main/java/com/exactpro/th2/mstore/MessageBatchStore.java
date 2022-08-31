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

import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.protobuf.TextFormat;
import com.exactpro.th2.common.grpc.ConnectionID;
import org.jetbrains.annotations.NotNull;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.mstore.cfg.MessageStoreConfiguration;

import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;

@Deprecated(since = "5.0.0")
public class MessageBatchStore extends AbstractMessageStore<MessageBatch, Message> {
    private static final String[] ATTRIBUTES = Stream.of(QueueAttribute.SUBSCRIBE, QueueAttribute.PARSED)
            .map(QueueAttribute::toString)
            .toArray(String[]::new);

    public MessageBatchStore(
            MessageRouter<MessageBatch> router,
            @NotNull CradleManager cradleManager,
            @NotNull MessageStoreConfiguration configuration
    ) {
        super(router, cradleManager, configuration);
    }

    @Override
    protected MessageToStore convert(Message originalMessage) {
        return ProtoUtil.toCradleMessage(originalMessage);
    }

    @Override
    protected CompletableFuture<Void> store(StoredMessageBatch storedMessageBatch) {
        return cradleStorage.storeProcessedMessageBatchAsync(storedMessageBatch);
    }

    @Override
    protected SequenceToTimestamp extractSequenceToTimestamp(Message message) {
        return new SequenceToTimestamp(
                message.getMetadata().getId().getSequence(),
                message.getMetadata().getTimestamp()
        );
    }

    @Override
    protected SessionKey createSessionKey(Message message) {
        MessageID messageID = message.getMetadata().getId();
        return new SessionKey(messageID.getConnectionId().getSessionAlias(), toCradleDirection(messageID.getDirection()));
    }

    @Override
    protected String[] getAttributes() {
        return ATTRIBUTES;
    }

    @Override
    protected List<Message> getMessages(MessageBatch delivery) {
        return delivery.getMessagesList();
    }

    @Override
    protected String shortDebugString(MessageBatch batch) {
        return batch.getMessagesList().stream()
                .map(message -> message.getMetadata().getId())
                .map(id -> {
                    ConnectionID connectionID = id.getConnectionId();
                    StringJoiner joiner = new StringJoiner(":");
                    joiner.add(connectionID.getSessionAlias());
                    joiner.add(id.getDirection().name());
                    joiner.add(String.valueOf(id.getSequence()));
                    return joiner.toString();
                }).collect(Collectors.joining(","));
    }
}
