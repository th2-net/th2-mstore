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

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static org.apache.commons.lang3.builder.ToStringStyle.NO_CLASS_NAME_STYLE;

public class RawMessageBatchStore extends AbstractMessageStore<RawMessageBatch, RawMessage> {
    private static final String[] ATTRIBUTES = Stream.of(QueueAttribute.SUBSCRIBE, QueueAttribute.RAW)
            .map(QueueAttribute::toString)
            .toArray(String[]::new);

    public RawMessageBatchStore(
            MessageRouter<RawMessageBatch> router,
            @NotNull CradleStorage cradleStorage,
            @NotNull Persistor<StoredMessageBatch> persistor,
            @NotNull Configuration configuration
    ) {
        super(router, cradleStorage, persistor, configuration);
    }

    @Override
    protected MessageToStore convert(RawMessage originalMessage) {
        return ProtoUtil.toCradleMessage(originalMessage);
    }

    @Override
    protected SequenceToTimestamp extractSequenceToTimestamp(RawMessage message) {
        return new SequenceToTimestamp(
                message.getMetadata().getId().getSequence(),
                message.getMetadata().getTimestamp()
        );
    }

    @Override
    protected SessionKey createSessionKey(RawMessage message) {
        MessageID messageID = message.getMetadata().getId();
        return new SessionKey(messageID.getConnectionId().getSessionAlias(), toCradleDirection(messageID.getDirection()));
    }

    @Override
    protected String[] getAttributes() {
        return ATTRIBUTES;
    }

    @Override
    protected List<RawMessage> getMessages(RawMessageBatch delivery) {
        return delivery.getMessagesList();
    }

    @Override
    protected String shortDebugString(RawMessageBatch batch) {
        return formatRawMessageBatch(batch, false);
    }

    public static String formatRawMessageBatch(RawMessageBatch messageBatch, boolean full) {
        int count = messageBatch.getMessagesCount();
        if (count == 0) {
            return "[]";
        }

        RawMessage first = messageBatch.getMessages(0);
        RawMessage last = messageBatch.getMessages(count - 1);
        ToStringBuilder builder = new ToStringBuilder(messageBatch, NO_CLASS_NAME_STYLE)
                .append("stream", MessageUtils.getSessionAlias(first))
                .append("direction", MessageUtils.getDirection(first))
                .append("batch id", MessageUtils.getSequence(first))
                .append("min timestamp", Timestamps.toString(first.getMetadata().getTimestamp()))
                .append("max timestamp", Timestamps.toString(last.getMetadata().getTimestamp()))
                .append("size", messageBatch.getMessagesList().stream()
                        .map(RawMessage::getBody)
                        .mapToInt(ByteString::size)
                        .sum())
                .append("count", count);
        if (full) {
            builder.append("sequences", messageBatch.getMessagesList().stream()
                    .map(RawMessage::getMetadata)
                    .map(RawMessageMetadata::getId)
                    .map(MessageID::getSequence)
                    .map(Objects::toString)
                    .collect(Collectors.joining(",", "[", "]")));
        }
        return builder.toString();
    }
}
