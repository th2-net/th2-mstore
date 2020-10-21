/******************************************************************************
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
 ******************************************************************************/
package com.exactpro.th2.messagestore;

import static com.exactpro.cradle.messages.StoredMessageBatch.MAX_MESSAGES_COUNT;
import static com.exactpro.th2.messagestore.MStoreConfiguration.readConfiguration;
import static javax.xml.bind.DatatypeConverter.printHexBinary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.cradle.CradleManager;
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
import com.exactpro.th2.store.common.utils.ProtoUtil;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.TextFormat;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

public class DemoMessageStore {
    private final static Logger LOGGER = LoggerFactory.getLogger(DemoMessageStore.class);
    private final MStoreConfiguration configuration;
    private final List<Subscriber> subscribers;
    private CradleManager cradleManager;

    public DemoMessageStore(MStoreConfiguration configuration) {
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
        boolean subscriptionFailure = false;
        for (Subscriber subscriber : subscribers) {
            try {
                subscriber.start();
            } catch (RuntimeException e) {
                LOGGER.error(e.getMessage(), e);
                subscriptionFailure = true;
            }
        }

        if (configuration.isCheckSubscriptionsOnStart() && subscriptionFailure) {
            throw new IllegalStateException("Some of subscription failure");
        }

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
        for (Map.Entry<String, QueueNames> queueNamesEntry : connectivityServices.entrySet()) {
            try {
                Subscriber subscriber = createSubscriber(rabbitMQ, queueNamesEntry.getValue(), queueNamesEntry.getKey());
                subscribers.add(subscriber);
            } catch (RuntimeException e) {
                LOGGER.error("Could not create a subscriber for '{}' box", queueNamesEntry.getKey(), e);
            }
        }

        if (configuration.isCheckSubscriptionsOnStart()
            && subscribers.size() != connectivityServices.size()) {
            throw new IllegalStateException("Problem with subscription: "
                    + "expaected " + connectivityServices.size() + ", "
                    + "actual " + subscribers.size());
        }

        return Collections.unmodifiableList(subscribers);
    }

    private Subscriber createSubscriber(RabbitMQConfiguration rabbitMQ, QueueNames queueNames, String boxAlias) {
        Map<String, RabbitMqSubscriber> subscriptionKeyToSubscriber = new HashMap<>();
        createRabbitMqSubscriber(subscriptionKeyToSubscriber, queueNames.getOutQueueName(),
                queueNames.getExchangeName(), this::storeMessageBatch);
        createRabbitMqSubscriber(subscriptionKeyToSubscriber, queueNames.getInQueueName(),
                queueNames.getExchangeName(), this::storeMessageBatch);
        createRabbitMqSubscriber(subscriptionKeyToSubscriber, queueNames.getOutRawQueueName(),
                queueNames.getExchangeName(), this::storeRawMessageBatch);
        createRabbitMqSubscriber(subscriptionKeyToSubscriber, queueNames.getInRawQueueName(),
                queueNames.getExchangeName(), this::storeRawMessageBatch);
        return new Subscriber(rabbitMQ, boxAlias, subscriptionKeyToSubscriber);
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
            storeMessages(messagesList, ProtoUtil::toCradleMessage, cradleManager.getStorage()::storeProcessedMessageBatch);
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
            storeMessages(messagesList, ProtoUtil::toCradleMessage, cradleManager.getStorage()::storeMessageBatch);
        } catch (CradleStorageException | IOException | RuntimeException e) {
            LOGGER.error("'{}':'{}' could not store message batch.",
                    delivery.getEnvelope().getExchange(), delivery.getEnvelope().getRoutingKey(), e);
            LOGGER.error("batch body: {}", printHexBinary(delivery.getBody()));
        }
    }

    private <T extends MessageLite> void storeMessages(List<T> messagesList, Function<T, MessageToStore> convertToMessageToStore,
                                                       CradleStoredMessageBatchFunction cradleStoredMessageBatchFunction) throws CradleStorageException, IOException {
        if (messagesList.isEmpty()) {
            LOGGER.warn("Empty batch has been received"); //FIXME: need identify
            return;
        }

        LOGGER.debug("Process {} messages started, max {}", messagesList.size(), MAX_MESSAGES_COUNT);

        StoredMessageBatch storedMessageBatch = new StoredMessageBatch();
        for (T message : messagesList) {
            storedMessageBatch.addMessage(convertToMessageToStore.apply(message));
        }
        cradleStoredMessageBatchFunction.store(storedMessageBatch);

        LOGGER.debug("Message Batch stored: stream '{}', direction '{}', id '{}', size '{}'",
                storedMessageBatch.getStreamName(), storedMessageBatch.getId().getDirection(), storedMessageBatch.getId().getIndex(), storedMessageBatch.getMessageCount());
    }

    @FunctionalInterface
    private interface CradleStoredMessageBatchFunction {
        void store(StoredMessageBatch storedMessageBatch) throws IOException;
    }

    private void createRabbitMqSubscriber(Map<String, RabbitMqSubscriber> subscriptionKeyToSubscriber, String routingKey, String exchangeName, DeliverCallback deliverCallback) {
        if (StringUtils.isNotEmpty(routingKey)) {
            String subscriptionKey = exchangeName + ':' + routingKey;
            if (subscriptionKeyToSubscriber.containsKey(subscriptionKey)) {
                LOGGER.warn("Subscriber for {} already exist", subscriptionKey);
            } else {
                LOGGER.debug("Subscriber created for '{}'", subscriptionKey);
                subscriptionKeyToSubscriber.put(subscriptionKey, new RabbitMqSubscriber(exchangeName, deliverCallback, null, routingKey));
            }
        }
    }

    private static class Subscriber {
        private final RabbitMQConfiguration rabbitMQ;
        private final String boxAlias;
        private final Map<String, RabbitMqSubscriber> subscriptionKeyToSubscriber;

        private Subscriber(RabbitMQConfiguration rabbitMQ, String boxAlias, Map<String, RabbitMqSubscriber> subscriptionKeyToSubscriber) {
            this.rabbitMQ = rabbitMQ;
            this.boxAlias = boxAlias;
            this.subscriptionKeyToSubscriber = Collections.unmodifiableMap(subscriptionKeyToSubscriber);
        }

        public String getBoxAlias() {
            return boxAlias;
        }

        public void start() {
            List<Throwable> suppressedExceptions = new ArrayList<>();

            for (Entry<String, RabbitMqSubscriber> pair : subscriptionKeyToSubscriber.entrySet()) {
                try {
                    pair.getValue().startListening(rabbitMQ.getHost(), rabbitMQ.getVirtualHost(), rabbitMQ.getPort(),
                            rabbitMQ.getUsername(), rabbitMQ.getPassword(), "mstore");
                    LOGGER.info("Suscribed to {}", pair.getKey());
                } catch (TimeoutException | IOException | RuntimeException e) {
                    suppressedExceptions.add(new IllegalStateException("Could not subscribe to '" + pair.getKey() + '\'', e));
                }
            }

            if (!suppressedExceptions.isEmpty()) {
                RuntimeException exception = new IllegalStateException("S``ubscription to routing keys of the '" + boxAlias + "' box failure. "
                        + "Expected " + subscriptionKeyToSubscriber.size()
                        + ", failured " + suppressedExceptions.size());

                for (Throwable suppressedException : suppressedExceptions) {
                    exception.addSuppressed(suppressedException);
                }

                throw exception;
            }
        }

        public void dispose() {
            for (Entry<String, RabbitMqSubscriber> pair : subscriptionKeyToSubscriber.entrySet()) {
                try {
                    pair.getValue().close();
                } catch (Exception e) {
                    LOGGER.error("Could not dispose the mq subscriber to '" + pair.getKey() + "' routing key", e);
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            MStoreConfiguration configuration = readConfiguration(args);
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
