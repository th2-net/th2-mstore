/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessage.Builder;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.message.MessageUtils;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static com.exactpro.th2.mstore.AbstractMessageStore.formatStoredMessageBatch;
import static com.exactpro.th2.mstore.RawMessageBatchStore.formatRawMessageBatch;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestMessageStore {

    private static final Instant TIMESTAMP = Instant.parse("2022-04-08T08:15:55.328451976Z");

    @Test
    public void testFormatMessageBatch() {
        Builder builder = RawMessage.newBuilder();
        builder.setBody(ByteString.copyFrom(new byte[] { 3, 1, 2 }))
                .getMetadataBuilder()
                    .setTimestamp(MessageUtils.toTimestamp(TIMESTAMP))
                    .getIdBuilder()
                        .setSequence(2)
                        .setDirection(com.exactpro.th2.common.grpc.Direction.FIRST)
                        .getConnectionIdBuilder()
                            .setSessionAlias("test-stream");

        RawMessage first = builder.build();

        builder.setBody(ByteString.copyFrom(new byte[] { 1, 3, 2 }))
                .getMetadataBuilder()
                    .setTimestamp(MessageUtils.toTimestamp(TIMESTAMP.plus(1, ChronoUnit.DAYS)))
                    .getIdBuilder()
                        .setSequence(3);

        RawMessage second = builder.build();

        RawMessageBatch batch = RawMessageBatch.newBuilder()
                .addMessages(first)
                .addMessages(second)
                .build();

        assertEquals("[stream=test-stream,direction=FIRST,batch id=2,min timestamp=2022-04-08T08:15:55.328451976Z,max timestamp=2022-04-09T08:15:55.328451976Z,size=6,count=2,sequences=[2,3]]", formatRawMessageBatch(batch, true));
        assertEquals("[stream=test-stream,direction=FIRST,batch id=2,min timestamp=2022-04-08T08:15:55.328451976Z,max timestamp=2022-04-09T08:15:55.328451976Z,size=6,count=2]", formatRawMessageBatch(batch, false));

        assertEquals("[]", formatRawMessageBatch(RawMessageBatch.getDefaultInstance(), true));
        assertEquals("[]", formatRawMessageBatch(RawMessageBatch.getDefaultInstance(), false));
    }

    @Test
    public void testFormatStoredMessageBatch() throws CradleStorageException {
        MessageToStore first = new MessageToStore();
        first.setDirection(Direction.FIRST);
        first.setStreamName("test-stream");

        MessageToStore second = new MessageToStore(first);
        second.setIndex(3);
        second.setTimestamp(TIMESTAMP.plus(1, ChronoUnit.SECONDS));
        second.setContent(new byte[] { 1, 3, 2 });

        first.setIndex(2);
        first.setTimestamp(TIMESTAMP);
        first.setContent(new byte[] { 3, 1, 2 });

        StoredMessageBatch batch = new StoredMessageBatch();
        batch.addMessage(first);
        batch.addMessage(second);

        assertEquals(
                "[stream=test-stream,direction=FIRST,batch id=2,min timestamp=2022-04-08T08:15:55.328451976Z,max timestamp=2022-04-08T08:15:56.328451976Z,size=97,count=2,sequences=[2,3]]",
                formatStoredMessageBatch(batch, true)
        );
        assertEquals(
                "[stream=test-stream,direction=FIRST,batch id=2,min timestamp=2022-04-08T08:15:55.328451976Z,max timestamp=2022-04-08T08:15:56.328451976Z,size=97,count=2]",
                formatStoredMessageBatch(batch, false)
        );

        StoredMessageBatch emptyBatch = new StoredMessageBatch();

        assertEquals("[]", formatStoredMessageBatch(emptyBatch, true));
        assertEquals("[]", formatStoredMessageBatch(emptyBatch, false));
    }
}
