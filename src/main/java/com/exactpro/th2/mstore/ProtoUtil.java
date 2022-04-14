/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.MessageToStoreBuilder;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.MessageMetadata;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;

import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static com.exactpro.th2.common.util.StorageUtils.toInstant;

import java.util.Map;

public class ProtoUtil {
    public static MessageToStore toCradleMessage(Message protoMessage) {
        MessageMetadata metadata = protoMessage.getMetadata();
        MessageID messageID = metadata.getId();
        var builder = new MessageToStoreBuilder()
                .streamName(messageID.getConnectionId().getSessionAlias())
                .content(protoMessage.toByteArray())
                .timestamp(toInstant(metadata.getTimestamp()))
                .direction(toCradleDirection(messageID.getDirection()))
                .index(messageID.getSequence());
        addProperties(builder, protoMessage.getMetadata().getPropertiesMap());
        return builder.build();
    }

    public static MessageToStore toCradleMessage(RawMessage protoRawMessage) {
        RawMessageMetadata metadata = protoRawMessage.getMetadata();
        MessageID messageID = metadata.getId();
        var builder = new MessageToStoreBuilder()
                .streamName(messageID.getConnectionId().getSessionAlias())
                .content(protoRawMessage.toByteArray())
                .timestamp(toInstant(metadata.getTimestamp()))
                .direction(toCradleDirection(messageID.getDirection()))
                .index(messageID.getSequence());
        addProperties(builder, protoRawMessage.getMetadata().getPropertiesMap());
        return builder.build();
    }

    private static void addProperties(MessageToStoreBuilder builder, Map<String, String> properties) {
        properties.forEach(builder::metadata);
    }
}
