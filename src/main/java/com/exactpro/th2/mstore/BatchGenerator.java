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
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.MessageToStoreBuilder;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.grpc.RawMessage;
import io.prometheus.client.Histogram;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import static java.util.Objects.requireNonNull;

public class BatchGenerator implements AutoCloseable  {
    private static final Logger logger = LoggerFactory.getLogger(BatchGenerator.class);

    protected final CradleStorage cradleStorage;
    private final Configuration configuration;
    private final Persistor<GroupedMessageBatchToStore> persistor;
    private final MessageProcessorMetrics metrics;

    private final BookId bookId = new BookId("first_book");
    private final Random random = new Random();

    public BatchGenerator(
            @NotNull CradleStorage cradleStorage,
            @NotNull Persistor<GroupedMessageBatchToStore> persistor,
            @NotNull Configuration configuration
    ) {
        this.cradleStorage = requireNonNull(cradleStorage, "Cradle storage can't be null");
        this.persistor = Objects.requireNonNull(persistor, "Persistor can't be null");
        this.configuration = Objects.requireNonNull(configuration, "'Configuration' parameter");
        this.metrics = new MessageProcessorMetrics();
    }

    public void start() throws CradleStorageException {
        logger.info("generating {} batches with {} messages in it with content size {}", configuration.getBatches(),
                configuration.getBatchSize(),
                configuration.getMessageSize());

        byte[][] contents = new byte[configuration.getBatchSize()][];
        for (int i = 0; i < configuration.getBatchSize(); i++) {
            contents[i] = new byte[configuration.getMessageSize()];
            random.nextBytes(contents[i]);
        }


        MutableLong sequence = new MutableLong(1L);
        for (int i = 0; i < configuration.getBatches(); i++) {
            GroupedMessageBatchToStore batch = new GroupedMessageBatchToStore("test-group", 2_000_000);
            for (int m = 0; m < configuration.getBatchSize(); m++) {
                batch.addMessage(generateMessage("mstore-gen-sesssion", Direction.SECOND, sequence, contents));
            }
            persist(batch);
        }
    }


    protected MessageToStore generateMessage (String sessionAlias, Direction direction, MutableLong sequence, byte[][] contents) throws CradleStorageException {

        return new MessageToStoreBuilder().bookId(bookId)
                .sessionAlias(sessionAlias)
                .direction(direction)
                .sequence(sequence.incrementAndGet())
                .protocol("x")
                .timestamp(Instant.now())
                .content(contents[random.nextInt(contents.length)])
                .build();
    }



    @Override
    public void close() {
    }



    private GroupedMessageBatchToStore toCradleBatch(String group, List<RawMessage> messagesList) throws CradleStorageException {
        GroupedMessageBatchToStore batch = cradleStorage.getEntitiesFactory().groupedMessageBatch(group);
        for (RawMessage message : messagesList) {
            MessageToStore messageToStore = ProtoUtil.toCradleMessage(message);
            batch.addMessage(messageToStore);
        }
        return batch;
    }

    private void persist(GroupedMessageBatchToStore batch) {
        Histogram.Timer timer = metrics.startMeasuringPersistenceLatency();
        try {
            persistor.persist(batch, new Callback<>() {
                @Override
                public void onSuccess(GroupedMessageBatchToStore batch) {
                }

                @Override
                public void onFail(GroupedMessageBatchToStore batch) {
                }
            });
        } catch (Exception e) {
            logger.error("Exception storing batch for group",e);
        } finally {
            timer.observeDuration();
        }
    }
}
