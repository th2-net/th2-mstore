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

import com.exactpro.cradle.BookId;
import com.exactpro.cradle.CradleStorage;
import com.exactpro.th2.common.grpc.*;
import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback;
import com.exactpro.th2.common.util.StorageUtils;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static java.util.Objects.requireNonNull;

public class MessageGenerator implements AutoCloseable  {
    private static final Logger logger = LoggerFactory.getLogger(MessageGenerator.class);

    protected final CradleStorage cradleStorage;
    private final Configuration configuration;
    private final MessageProcessor processor;

    private final BookId bookId = new BookId("first_book");
    private final String sessionAlias = "mstore-gen-sessions";
    private final Direction direction = Direction.SECOND;

    private final Random random = new Random();

    public MessageGenerator(
            @NotNull CradleStorage cradleStorage,
            @NotNull MessageProcessor processor,
            @NotNull Configuration configuration
    ) {
        this.cradleStorage = requireNonNull(cradleStorage, "Cradle storage can't be null");
        this.processor = processor;
        this.configuration = Objects.requireNonNull(configuration, "'Configuration' parameter");
    }

    public void start() throws Exception {
        logger.info("generating {} batches with {} messages in it with content size {}", configuration.getBatches(),
                configuration.getBatchSize(),
                configuration.getMessageSize());

        byte[][] contents = new byte[configuration.getBatchSize()][];
        for (int i = 0; i < configuration.getBatchSize(); i++) {
            contents[i] = new byte[configuration.getMessageSize()];
            random.nextBytes(contents[i]);
        }

        ManualAckDeliveryCallback.Confirmation confirmation = new ManualAckDeliveryCallback.Confirmation() {
            @Override
            public void confirm() throws IOException {
            }

            @Override
            public void reject() throws IOException {
            }
        };
        MutableLong sequence = new MutableLong(cradleStorage.getLastSequence(sessionAlias, StorageUtils.toCradleDirection(direction), bookId));

        for (int i = 0; i < configuration.getBatches(); i++) {
            List<RawMessage> messages = new ArrayList<>();
            for (int m = 0; m < configuration.getBatchSize(); m++) {
                messages.add(generateRawMessage(sessionAlias, direction, sequence, contents));
            }
            processor.process(
                    new DeliveryMetadata("", false),
                    RawMessageBatch.newBuilder().addAllMessages(messages).build(),
                    confirmation);
        }
    }


    private RawMessage generateRawMessage (String sessionAlias, Direction direction, MutableLong sequence, byte[][] contents) {
        var metadataBuilder = RawMessageMetadata.newBuilder()
                .setId(MessageID.newBuilder()
                        .setBookName(bookId.getName())
                        .setConnectionId(ConnectionID.newBuilder()
                                .setSessionAlias(sessionAlias)
                                .setSessionGroup("test-group")
                                .build())
                        .setTimestamp(Timestamps.fromMillis(Instant.now().toEpochMilli()))
                        .setDirection(direction)
                        .setSequence(sequence.incrementAndGet())
                        .build())
                .setProtocol("X");

        return RawMessage.newBuilder()
                .setBody(ByteString.copyFrom(contents[random.nextInt(contents.length)]))
                .setMetadata(metadataBuilder.build())
                .build();
    }


    @Override
    public void close() {
    }
}
