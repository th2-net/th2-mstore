/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.ConnectionID;
import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.util.StorageUtils;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;

class TestProtoUtil {

    @Test
    public void testToCradleMessage() throws CradleStorageException {
        RawMessage rawMessage = RawMessage.newBuilder()
                .setMetadata(RawMessageMetadata.newBuilder()
                        .setProtocol("test-protocol")
                        .putProperties("test-key", "test-value")
                        .setId(MessageID.newBuilder()
                                .setConnectionId(ConnectionID.newBuilder()
//                                        .setSessionGroup("test-session-group") // this paremter isn't present in MessageToStore
                                        .setSessionAlias("test-session-alias")
                                        .build())
                                .setBookName("test-book")
                                .setTimestamp(MessageUtils.toTimestamp(Instant.now()))
                                .setDirection(Direction.SECOND)
                                .setSequence(6234)
                                .build())
                        .build())
                .setBody(ByteString.copyFrom(new byte[] {1, 4, 2, 3}))
                .build();

        MessageToStore cradleMessage = ProtoUtil.toCradleMessage(rawMessage);

        Assertions.assertEquals(rawMessage.getMetadata().getProtocol(), cradleMessage.getProtocol());
        Assertions.assertEquals(rawMessage.getMetadata().getPropertiesMap(), cradleMessage.getMetadata().toMap());
        Assertions.assertEquals(rawMessage.getMetadata().getId().getConnectionId().getSessionAlias(), cradleMessage.getSessionAlias());
        Assertions.assertEquals(rawMessage.getMetadata().getId().getBookName(), cradleMessage.getBookId().getName());
        Assertions.assertEquals(StorageUtils.toCradleDirection(rawMessage.getMetadata().getId().getDirection()), cradleMessage.getDirection());
        Assertions.assertEquals(StorageUtils.toInstant(rawMessage.getMetadata().getId().getTimestamp()), cradleMessage.getTimestamp());
        Assertions.assertEquals(rawMessage.getMetadata().getId().getSequence(), cradleMessage.getSequence());
        Assertions.assertArrayEquals(rawMessage.getBody().toByteArray(), cradleMessage.getContent());
    }
}