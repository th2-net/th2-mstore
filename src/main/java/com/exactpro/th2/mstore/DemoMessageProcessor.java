/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.cradle.messages.GroupedMessageBatchToStore;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.MessageToStoreBuilder;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.common.message.MessageUtils;
import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback.Confirmation;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoDirection;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoGroupBatch;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessage;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessageGroup;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoMessageId;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.demo.DemoRawMessage;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DemoMessageProcessor extends AbstractMessageProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DemoMessageProcessor.class);
    private final MessageRouter<DemoGroupBatch> router;
    private SubscriberMonitor monitor;

    public DemoMessageProcessor(
            @NotNull MessageRouter<DemoGroupBatch> router,
            @NotNull CradleStorage cradleStorage,
            @NotNull Persistor<GroupedMessageBatchToStore> persistor,
            @NotNull Configuration configuration,
            @NotNull Integer prefetchCount
    ) {
        super(cradleStorage, persistor, configuration, prefetchCount);
        this.router = requireNonNull(router, "Message router can't be null");
    }

    public void start() {
        if (monitor == null) {

            monitor = router.subscribeAllWithManualAck(this::process, AbstractMessageProcessor.ATTRIBUTES);
            if (monitor != null) {
                LOGGER.info("RabbitMQ subscription was successful");
            } else {
                LOGGER.error("Can not find queues for subscribe");
                throw new RuntimeException("Can not find queues for subscriber");
            }
        }
        super.start();

    }


    @Override
    public void close() {
        if (monitor != null) {
            try {
                monitor.unsubscribe();
            } catch (Exception e) {
                LOGGER.error("Can not unsubscribe from queues", e);
            }
        }
        super.close();
    }


    void process(DeliveryMetadata deliveryMetadata, DemoGroupBatch messageBatch, Confirmation confirmation) {
        try {
            List<DemoMessageGroup> messages = messageBatch.getGroups();
            if (messages.isEmpty()) {
                LOGGER.warn("Received empty batch {}", messageBatch);
                confirm(confirmation);
                return;
            }

            if (!deliveryMetadata.isRedelivered()) {
                verifyBatch(messages);
            }

            String group = createSessionKey((DemoRawMessage) messages.get(0).getMessages().get(0)).sessionGroup;
            GroupedMessageBatchToStore groupedMessageBatchToStore = toCradleBatch(group, messages);

            for (DemoMessageGroup demoMessageGroup: messages) {
                DemoRawMessage demoRawMessage = (DemoRawMessage) demoMessageGroup.getMessages().get(0);
                SessionKey sessionKey = createSessionKey(demoRawMessage);
                MessageOrderingProperties sequenceToTimestamp = extractOrderingProperties(demoRawMessage);
                sessions.computeIfAbsent(sessionKey, k -> new SessionData(sequenceToTimestamp));
            }

            if (deliveryMetadata.isRedelivered()) {
                persist(new ConsolidatedBatch(groupedMessageBatchToStore, confirmation));
            } else {
                storeMessages(groupedMessageBatchToStore, confirmation);
            }
        } catch (Exception ex) {
            LOGGER.error("Cannot handle the batch of type {}, rejecting", messageBatch.getClass(), ex);
            reject(confirmation);
        }
    }

    protected MessageOrderingProperties extractOrderingProperties(DemoRawMessage message) {
        return new MessageOrderingProperties(
                message.getId().getSequence(),
                MessageUtils.toTimestamp(message.getId().getTimestamp()) //FIXME: redundant convertation
        );
    }

    private static void confirm(Confirmation confirmation) {
        try {
            confirmation.confirm();
        } catch (Exception e) {
            LOGGER.error("Exception confirming message", e);
        }
    }


    private static void reject(Confirmation confirmation) {
        try {
            confirmation.reject();
        } catch (Exception e) {
            LOGGER.error("Exception rejecting message", e);
        }
    }


    //FIXME: com.exactpro.th2.mstore.MessageProcessor.toCradleBatch() 98,242 ms (40.5%)
    private GroupedMessageBatchToStore toCradleBatch(String group, List<DemoMessageGroup> messagesList) throws CradleStorageException {
        GroupedMessageBatchToStore batch = cradleStorage.getEntitiesFactory().groupedMessageBatch(group);
        for (DemoMessageGroup demoMessageGroup : messagesList) {
            MessageToStore messageToStore = toCradleMessage((DemoRawMessage)demoMessageGroup.getMessages().get(0));
            batch.addMessage(messageToStore);
        }
        return batch;
    }

    private void verifyBatch(List<DemoMessageGroup> messages) {
        Map<SessionKey, SessionData> localCache = new HashMap<>();
        SessionKey firstSessionKey = null;
        for (int i = 0; i < messages.size(); i++) {
            DemoMessageGroup demoMessageGroup = messages.get(i);
            if (demoMessageGroup.getMessages().size() != 1) {
                throw new IllegalArgumentException("Demo message group (" + i +
                        ") contains more than one message: " + demoMessageGroup);
            }
            DemoMessage<?> demoMessage = demoMessageGroup.getMessages().get(0);
            if ( !(demoMessage instanceof DemoRawMessage)) {
                throw new IllegalArgumentException("Demo message (" + i +
                        ") has incorrect type, expected: " + DemoRawMessage.class.getSimpleName() +
                        ", actual: " + demoMessageGroup.getClass().getSimpleName());
            }
            DemoRawMessage message = (DemoRawMessage) demoMessage;

            SessionKey sessionKey = createSessionKey(message);
            if (firstSessionKey == null) {
                firstSessionKey = sessionKey;
            }

            if(!firstSessionKey.sessionGroup.equals(sessionKey.sessionGroup)){
                throw new IllegalArgumentException(format(
                        "Delivery contains different session groups. Message [%d] - sequence %s; Message [%d] - sequence %s",
                        i - 1,
                        firstSessionKey,
                        i,
                        sessionKey
                ));
            }

            MessageOrderingProperties orderingProperties = extractOrderingProperties(message);
            SessionData sessionData = localCache.get(sessionKey);
            if (sessionData == null) {
                sessionData = sessions.get(sessionKey);
            }
            MessageOrderingProperties lastOrderingProperties = sessionData == null
                    ? loadLastOrderingProperties(sessionKey)
                    : sessionData.getLastOrderingProperties();

            verifyOrderingProperties(i, lastOrderingProperties, orderingProperties);
            localCache.put(sessionKey, new SessionData(orderingProperties));
        }
    }

    private SessionKey createSessionKey(DemoRawMessage message) {
        DemoMessageId messageId = message.getId();
        return new SessionKey(messageId.getBook(),
                messageId.getSessionGroup(),
                messageId.getSessionAlias(),
                toCradleDirection(messageId.getDirection()));
    }


    public static com.exactpro.cradle.Direction toCradleDirection(DemoDirection demoDirection) {
        switch (demoDirection) {
            case INCOMING: return com.exactpro.cradle.Direction.FIRST;
            case OUTGOING: return com.exactpro.cradle.Direction.SECOND;
            default: throw new IllegalStateException("Unknown demo direction " + demoDirection);
        }
    }

    private static MessageToStore toCradleMessage(DemoRawMessage demoRawMessage) throws CradleStorageException {
        return createMessageToStore(
                demoRawMessage.getId(),
                demoRawMessage.getProtocol(),
                demoRawMessage.getBody()
        );
    }

    private static MessageToStore createMessageToStore(DemoMessageId messageId, String protocol, byte[] body) throws CradleStorageException {
        return new MessageToStoreBuilder()
                .bookId(new BookId(messageId.getBook()))
                .sessionAlias(messageId.getSessionAlias())
                .direction(toCradleDirection(messageId.getDirection()))
                .timestamp(messageId.getTimestamp())
                .sequence(messageId.getSequence())
                .protocol(protocol)
                .content(body == null ? new byte[0] : body)
                .build();
    }
}
