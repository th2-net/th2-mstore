/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.messagestore;

import static com.exactpro.cradle.messages.StoredMessageBatch.MAX_MESSAGES_COUNT;
import static com.exactpro.th2.store.common.Configuration.readConfiguration;
import static javax.xml.bind.DatatypeConverter.printHexBinary;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.cassandra.CassandraCradleManager;
import com.exactpro.cradle.cassandra.connection.CassandraConnection;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.RabbitMqSubscriber;
import com.exactpro.th2.configuration.RabbitMQConfiguration;
import com.exactpro.th2.configuration.Th2Configuration.QueueNames;
import com.exactpro.th2.infra.grpc.Message;
import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.infra.grpc.MessageMetadata;
import com.exactpro.th2.infra.grpc.RawMessage;
import com.exactpro.th2.infra.grpc.RawMessageBatch;
import com.exactpro.th2.infra.grpc.RawMessageMetadata;
import com.exactpro.th2.store.common.CassandraConfig;
import com.exactpro.th2.store.common.Configuration;
import com.exactpro.th2.store.common.utils.ProtoUtil;
import com.google.protobuf.MessageLite;
import com.google.protobuf.TextFormat;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

public class DemoMessageStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(DemoMessageStore.class);
    private final Configuration configuration;
    private final List<Subscriber> subscribers;
    private final Map<Pair<String, Direction>, AtomicLong> sessionToLastSequenceRaw = new ConcurrentHashMap<>();
    private final Map<Pair<String, Direction>, AtomicLong> sessionToLastSequenceParsed = new ConcurrentHashMap<>();
    private CradleManager cradleManager;

    public DemoMessageStore(Configuration configuration) {
        this.configuration = configuration;
        this.subscribers = createSubscribers(configuration.getRabbitMQ(), configuration.getSourceNameToQueueNames());
    }

    public void init() throws CradleStorageException {
        CassandraConfig cassandraConfig = configuration.getCassandraConfig();
        cradleManager = new CassandraCradleManager(new CassandraConnection(cassandraConfig.getConnectionSettings()));
        cradleManager.init(configuration.getCradleInstanceName());
        LOGGER.info("cradle init successfully with {} instance name", configuration.getCradleInstanceName() );
    }

    public void startAndBlock() throws InterruptedException {
        subscribers.forEach(Subscriber::start);
        LOGGER.info("message store started");
        synchronized (this) {
            wait();
        }
    }

    public void dispose() {
        subscribers.forEach(Subscriber::dispose);
        try {
            cradleManager.dispose();
        } catch (CradleStorageException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private List<Subscriber> createSubscribers(RabbitMQConfiguration rabbitMQ,
                                               Map<String, QueueNames> connectivityServices) {
        List<Subscriber> subscribers = new ArrayList<>();
        connectivityServices.forEach((key, value) -> {
            Subscriber subscriber = createSubscriber(rabbitMQ, value, key);
            if (subscriber != null) {
                subscribers.add(subscriber);
            }
        });
        return Collections.unmodifiableList(subscribers);
    }

    private @Nullable Subscriber createSubscriber(RabbitMQConfiguration rabbitMQ, QueueNames queueNames, String key) {
        try {
            RabbitMqSubscriber outMsgSubscriber = createRabbitMqSubscriber(queueNames.getOutQueueName(),
                    queueNames.getExchangeName(), this::storeMessageBatch);
            RabbitMqSubscriber inMsgSubscriber = createRabbitMqSubscriber(queueNames.getInQueueName(),
                    queueNames.getExchangeName(), this::storeMessageBatch);
            RabbitMqSubscriber outRawMsgSubscriber = createRabbitMqSubscriber(queueNames.getOutRawQueueName(),
                    queueNames.getExchangeName(), this::storeRawMessageBatch);
            RabbitMqSubscriber inRawMsgSubscriber = createRabbitMqSubscriber(queueNames.getInRawQueueName(),
                    queueNames.getExchangeName(), this::storeRawMessageBatch);
            return new Subscriber(rabbitMQ, inMsgSubscriber, outMsgSubscriber, inRawMsgSubscriber, outRawMsgSubscriber);
        } catch (RuntimeException e) {
            LOGGER.error("Could not create subscriber for '{}' connectivity", key, e);
        }
        return null;
    }

    private void storeMessageBatch(String consumerTag, Delivery delivery) {
        try {
            MessageBatch batch = MessageBatch.parseFrom(delivery.getBody());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Proseccing message batch " + batch.getMessagesList().stream()
                        .map(Message::getMetadata)
                        .map(MessageMetadata::getId)
                        .map(TextFormat::shortDebugString)
                        .collect(Collectors.toList()));
            }
            List<Message> messagesList = batch.getMessagesList();
            storeMessages(messagesList, ProtoUtil::toCradleMessage, this::checkSequenceParsed, cradleManager.getStorage()::storeProcessedMessageBatch);
        } catch (CradleStorageException | IOException | RuntimeException e) {
            LOGGER.error("'{}':'{}' could not store message.",
                    delivery.getEnvelope().getExchange(), delivery.getEnvelope().getRoutingKey(), e);
            LOGGER.error("message body: {}", printHexBinary(delivery.getBody()));
        }
    }

    private void storeRawMessageBatch(String consumerTag, Delivery delivery) {
        try {
            RawMessageBatch batch = RawMessageBatch.parseFrom(delivery.getBody());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Proseccing raw batch " + batch.getMessagesList().stream()
                        .map(RawMessage::getMetadata)
                        .map(RawMessageMetadata::getId)
                        .map(TextFormat::shortDebugString)
                        .collect(Collectors.toList()));
            }
            List<RawMessage> messagesList = batch.getMessagesList();
            storeMessages(messagesList, ProtoUtil::toCradleMessage, this::checkSequenceRaw, cradleManager.getStorage()::storeMessageBatch);
        } catch (CradleStorageException | IOException | RuntimeException e) {
            LOGGER.error("'{}':'{}' could not store message batch.",
                    delivery.getEnvelope().getExchange(), delivery.getEnvelope().getRoutingKey(), e);
            LOGGER.error("batch body: {}", printHexBinary(delivery.getBody()));
        }
    }

    private static <T extends MessageLite> void storeMessages(List<T> messagesList, Function<T, MessageToStore> convertToMessageToStore, Consumer<MessageToStore> checkSequence,
            CradleStoredMessageBatchFunction cradleStoredMessageBatchFunction) throws CradleStorageException, IOException {
        if (messagesList.isEmpty()) {
            LOGGER.warn("Empty batch has been received"); //FIXME: need identify
            return;
        }

        LOGGER.debug("Process {} messages started, max {}", messagesList.size(), MAX_MESSAGES_COUNT);

        StoredMessageBatch storedMessageBatch = new StoredMessageBatch();
        for (T message : messagesList) {
            MessageToStore messageToStore = convertToMessageToStore.apply(message);
            checkSequence.accept(messageToStore);
            storedMessageBatch.addMessage(messageToStore);
        }
        cradleStoredMessageBatchFunction.store(storedMessageBatch);

        LOGGER.debug("Message Batch stored: stream '{}', direction '{}', id '{}', size '{}'",
                storedMessageBatch.getStreamName(), storedMessageBatch.getId().getDirection(), storedMessageBatch.getId().getIndex(), storedMessageBatch.getMessageCount());
    }

    private static void checkSequence(Map<Pair<String, Direction>, AtomicLong> sessionToLastSequence, CradleLastSequnceFunction lastSequnceFunction, MessageToStore messageToStore) {
        long lastSequenceNumber = sessionToLastSequence
                .computeIfAbsent(createSessessionKey(messageToStore), sessionKey -> {
                    try {
                        long lastMessageIndex = lastSequnceFunction.getLastSequence(sessionKey.getLeft(), sessionKey.getRight());
                        LOGGER.info("Last message index for the {} session is {}", sessionKey, lastMessageIndex);
                        return new AtomicLong(lastMessageIndex);
                    } catch (IOException e) {
                        String error = "Couldn't get last message index for the " + sessionKey + " session";
                        LOGGER.error(error, e);
                        throw new RuntimeException(error, e);
                    }
                })
                .getAndAccumulate(messageToStore.getIndex(), Math::max);
        if (lastSequenceNumber >= messageToStore.getIndex()) {
            throw new RuntimeException("Sequeance '" + messageToStore.getStreamName() + ':' + messageToStore.getDirection() + ':' + messageToStore.getIndex()
                    + "' of stored message should be greater than " + lastSequenceNumber);
        }
    }

    private void checkSequenceRaw(MessageToStore messageToStore) {
        checkSequence(sessionToLastSequenceRaw, cradleManager.getStorage()::getLastMessageIndex, messageToStore);
    }

    private void checkSequenceParsed(MessageToStore messageToStore) {
        checkSequence(sessionToLastSequenceParsed, cradleManager.getStorage()::getLastProcessedMessageIndex, messageToStore);
    }

    private static Pair<String, Direction> createSessessionKey(MessageToStore messageToStore) {
        return new ImmutablePair<>(messageToStore.getStreamName(), messageToStore.getDirection());
    }

    @FunctionalInterface
    private interface CradleStoredMessageBatchFunction {
        void store(StoredMessageBatch storedMessageBatch) throws IOException;
    }

    @FunctionalInterface
    private interface CradleLastSequnceFunction {
        long getLastSequence(String sessionAlias, Direction direction) throws IOException;
    }

    private static @Nullable RabbitMqSubscriber createRabbitMqSubscriber(String queueName, String exchangeName, DeliverCallback deliverCallback) {
        if (StringUtils.isEmpty(queueName)) {
            return null;
        }
        LOGGER.info("Subscriber created for '{}':'{}'", exchangeName, queueName);
        return new RabbitMqSubscriber(exchangeName, deliverCallback, null, queueName);
    }

    private static class Subscriber {
        private final RabbitMQConfiguration rabbitMQ;
        private final RabbitMqSubscriber inSubscriber;
        private final RabbitMqSubscriber outSubscriber;
        private final RabbitMqSubscriber inRawSubscriber;
        private final RabbitMqSubscriber outRawSubscriber;

        private Subscriber(RabbitMQConfiguration rabbitMQ, @Nullable RabbitMqSubscriber inSubscriber,
                @Nullable RabbitMqSubscriber outSubscriber, @Nullable RabbitMqSubscriber inRawMsgSubscriber,
                @Nullable RabbitMqSubscriber outRawMsgSubscriber) {
            this.rabbitMQ = rabbitMQ;
            this.inSubscriber = inSubscriber;
            this.outSubscriber = outSubscriber;
            this.inRawSubscriber = inRawMsgSubscriber;
            this.outRawSubscriber = outRawMsgSubscriber;
        }

        private void start() {
            subscribe(inSubscriber);
            subscribe(outSubscriber);
            subscribe(inRawSubscriber);
            subscribe(outRawSubscriber);
        }

        private void dispose() {
            dispose(inSubscriber);
            dispose(outSubscriber);
            dispose(inRawSubscriber);
            dispose(outRawSubscriber);
        }

        private static void dispose(@Nullable Closeable subscriber) {
            if (subscriber == null) {
                return;
            }
            try {
                subscriber.close();
            } catch (IOException | RuntimeException e) {
                LOGGER.error("Could not dispose the mq subscriber", e);
            }
        }

        private void subscribe(@Nullable RabbitMqSubscriber subscriber) {
            if (subscriber == null) {
                return;
            }
            try {
                subscriber.startListening(rabbitMQ.getHost(), rabbitMQ.getVirtualHost(), rabbitMQ.getPort(),
                        rabbitMQ.getUsername(), rabbitMQ.getPassword());
            } catch (IOException | TimeoutException | RuntimeException e) {
                LOGGER.error("Could not subscribe to queue", e);
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration configuration = readConfiguration(args);
            DemoMessageStore messageStore = new DemoMessageStore(configuration);
            messageStore.init();
            messageStore.startAndBlock();
            Runtime.getRuntime().addShutdownHook(new Thread(messageStore::dispose));
        } catch (CradleStorageException | InterruptedException | RuntimeException e) {
            LOGGER.error(e.getMessage(), e);
            LOGGER.error("Error occurred. Exit the program");
            System.exit(-1);
        }
    }
}
