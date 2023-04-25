/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.mstore;

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.MessageToStoreBuilder;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.MessageIDOrBuilder;
import com.exactpro.th2.common.grpc.RawMessageOrBuilder;
import com.google.protobuf.ByteString;

import java.util.Map;

import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static com.exactpro.th2.common.util.StorageUtils.toInstant;

public class ProtoUtil {
    public static final byte[] EMPTY_CONTENT = new byte[0];

    public static MessageToStore toCradleMessage(RawMessageOrBuilder protoRawMessage) throws CradleStorageException {
        return createMessageToStore(
                protoRawMessage.getMetadata().getId(),
                protoRawMessage.getMetadata().getProtocol(),
                protoRawMessage.getMetadata().getPropertiesMap(),
                protoRawMessage.getBody()
        );
    }

    private static MessageToStore createMessageToStore(
            MessageIDOrBuilder messageId,
            String protocol,
            Map<String, String> propertiesMap,
            ByteString body
    ) throws CradleStorageException {
        MessageToStoreBuilder builder = new MessageToStoreBuilder()
                .bookId(new BookId(messageId.getBookName()))
                .sessionAlias(messageId.getConnectionId().getSessionAlias())
                .direction(toCradleDirection(messageId.getDirection()))
                .timestamp(toInstant(messageId.getTimestamp()))
                .sequence(messageId.getSequence())
                .protocol(protocol)
                .content(body == null ? EMPTY_CONTENT : body.toByteArray());

        propertiesMap.forEach(builder::metadata);
        return builder.build();
    }
}
