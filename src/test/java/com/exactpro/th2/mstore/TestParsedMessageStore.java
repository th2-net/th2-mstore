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

import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.google.protobuf.Timestamp;

import static com.exactpro.th2.common.message.MessageUtils.toTimestamp;

public class TestParsedMessageStore extends TestCaseMessageStore<MessageBatch, Message> {

    @Override
    protected AbstractMessageStore<MessageBatch, Message> createStore(CradleStorage cradleStorage, MessageRouter<MessageBatch> routerMock,
                                                                      Persistor<StoredMessageBatch> persistor, Configuration configuration) {
        return new ParsedMessageBatchStore(routerMock, cradleStorage, persistor, configuration);
    }

    @Override
    protected Message createMessage(String session, Direction direction, long sequence, Instant timestamp) {
        return Message.newBuilder()
                .setMetadata(
                        MessageMetadata.newBuilder()
                                .setMessageType("A")
                                .setId(createMessageId(session, direction, sequence))
                                .setTimestamp(toTimestamp(timestamp))
                                .build()
                )
                .build();
    }

    @Override
    protected long extractSizeInBatch(Message message) {
        return MessagesSizeCalculator.calculateMessageSizeInBatch(ProtoUtil.toCradleMessage(message));
    }

    @Override
    protected MessageBatch createDelivery(List<Message> messages) {
        return MessageBatch.newBuilder()
                .addAllMessages(messages)
                .build();
    }

    @Override
    protected Timestamp extractTimestamp(Message message) {
        return message.getMetadata().getTimestamp();
    }
}
