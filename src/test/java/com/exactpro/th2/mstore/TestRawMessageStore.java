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

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.cradle.serialization.MessagesSizeCalculator;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.mstore.cfg.MessageStoreConfiguration;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.List;

import static com.exactpro.th2.common.message.MessageUtils.toTimestamp;

public class TestRawMessageStore extends TestCaseMessageStore<RawMessageBatch, RawMessage> {
    TestRawMessageStore() {
        super(CradleStorage::storeGroupedMessageBatchAsync);
    }

    @Override
    protected AbstractMessageStore<RawMessageBatch, RawMessage> createStore(CradleManager cradleManagerMock, MessageRouter<RawMessageBatch> routerMock,
                                                                            MessageStoreConfiguration configuration) {
        return new RawMessageBatchStore(routerMock, cradleManagerMock, configuration);
    }

    @Override
    protected RawMessage createMessage(String session, String group, Direction direction, long sequence, Instant timestamp) {
        return RawMessage.newBuilder()
                .setMetadata(
                        RawMessageMetadata.newBuilder()
                                .setId(createMessageId(session, group, direction, sequence))
                                .setTimestamp(toTimestamp(timestamp))
                                .build()
                )
                .build();
    }

    @Override
    protected long extractSizeInBatch(RawMessage message) {
        return MessagesSizeCalculator.calculateMessageSizeInGroupBatch(ProtoUtil.toCradleMessage(message));
    }

    @Override
    protected RawMessageBatch createDelivery(List<RawMessage> messages) {
        return RawMessageBatch.newBuilder()
                .addAllMessages(messages)
                .build();
    }

    @Override
    protected Timestamp extractTimestamp(RawMessage message) {
        return message.getMetadata().getTimestamp();
    }
}
