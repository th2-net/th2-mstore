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
import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback.Confirmation;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Message;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.RawMessage;
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.TransportUtilsKt;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TransportGroupProcessor extends AbstractMessageProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransportGroupProcessor.class);
    private final MessageRouter<GroupBatch> router;
    private SubscriberMonitor monitor;

    public TransportGroupProcessor(
            @NotNull ErrorCollector errorCollector,
            @NotNull MessageRouter<GroupBatch> router,
            @NotNull CradleStorage cradleStorage,
            @NotNull Persistor<GroupedMessageBatchToStore> persistor,
            @NotNull Configuration configuration,
            @NotNull Integer prefetchCount
    ) {
        super(errorCollector, cradleStorage, persistor, configuration, prefetchCount);
        this.router = requireNonNull(router, "Message router can't be null");
    }

    public void start() {
        if (monitor == null) {
            monitor = router.subscribeAllWithManualAck(this::process);
            LOGGER.info("RabbitMQ subscription was successful");
        }
        super.start();

    }


    @Override
    public void close() {
        if (monitor != null) {
            try {
                monitor.unsubscribe();
            } catch (Exception e) {
                errorCollector.collect(LOGGER, "Can not unsubscribe from queues", e);
            }
        }
        super.close();
    }


    void process(DeliveryMetadata deliveryMetadata, GroupBatch messageBatch, Confirmation confirmation) {
        try {
            List<MessageGroup> messages = messageBatch.getGroups();
            if (messages.isEmpty()) {
                LOGGER.warn("Received empty batch {}", messageBatch);
                confirm(confirmation);
                return;
            }

            GroupKey groupKey = createGroupKey(messageBatch);

            if (!deliveryMetadata.isRedelivered()) {
                verifyBatch(groupKey, messageBatch);
            }

            GroupedMessageBatchToStore groupedMessageBatchToStore = toCradleBatch(messageBatch);

            for (MessageGroup messageGroup : messages) {
                Message<?> message = messageGroup.getMessages().get(0);
                SessionKey sessionKey = createSessionKey(messageBatch, message.getId());
                MessageOrderingProperties sequenceToTimestamp = extractOrderingProperties(message.getId());
                sessions.computeIfAbsent(sessionKey, key -> sequenceToTimestamp);
            }
            groups.put(groupKey, groupedMessageBatchToStore.getLastTimestamp());

            if (deliveryMetadata.isRedelivered()) {
                persist(new ConsolidatedBatch(groupedMessageBatchToStore, confirmation));
            } else {
                storeMessages(groupedMessageBatchToStore, groupKey, confirmation);
            }
        } catch (Exception ex) {
            GroupKey group = createGroupKey(messageBatch);
            String errorMessage = "Cannot handle the batch of type " + messageBatch.getClass()
                    + " for group '" + group + "', rejecting";
            errorCollector.collect(LOGGER, errorMessage, ex);
            reject(confirmation);
        }
    }

    protected MessageOrderingProperties extractOrderingProperties(MessageId messageId) {
        return new MessageOrderingProperties(
                messageId.getSequence(),
                messageId.getTimestamp()
        );
    }

    private GroupedMessageBatchToStore toCradleBatch(GroupBatch groupBatch) throws CradleStorageException {
        GroupedMessageBatchToStore batch = cradleStorage.getEntitiesFactory().groupedMessageBatch(groupBatch.getSessionGroup());
        for (MessageGroup messageGroup : groupBatch.getGroups()) {
            MessageToStore messageToStore = toCradleMessage(groupBatch.getBook(), (RawMessage) messageGroup.getMessages().get(0));
            batch.addMessage(messageToStore);
        }
        return batch;
    }

    private void verifyBatch(GroupKey groupKey, GroupBatch groupBatch) {
        List<MessageGroup> messages = groupBatch.getGroups();
        Map<SessionKey, MessageOrderingProperties> localCache = new HashMap<>();

        Instant lastGroupTimestamp = groups.get(groupKey);
        if (lastGroupTimestamp == null) {
            var bookId = new BookId(groupKey.bookName);
            lastGroupTimestamp = loadLastMessageTimestamp(bookId, groupKey.sessionGroup);
        }

        for (int i = 0; i < messages.size(); i++) {
            MessageGroup messageGroup = messages.get(i);
            if (messageGroup.getMessages().size() != 1) {
                throw new IllegalArgumentException("transport message group (" + i +
                        ") contains more than one message: " + messageGroup);
            }
            Message<?> message = messageGroup.getMessages().get(0);
            if (!(message instanceof RawMessage)) {
                throw new IllegalArgumentException("Transport message (" + i +
                        ") has incorrect type, expected: " + RawMessage.class.getSimpleName() +
                        ", actual: " + messageGroup.getClass().getSimpleName());
            }
            Instant messageTimestamp = message.getId().getTimestamp();
            if (lastGroupTimestamp.isAfter(messageTimestamp)) {
                throw new IllegalArgumentException(format(
                        "Received grouped batch `%s` containing message with timestamp %s, but previously was %s",
                        groupKey.sessionGroup,
                        messageTimestamp,
                        lastGroupTimestamp
                ));
            }
            SessionKey sessionKey = createSessionKey(groupBatch, message.getId());

            MessageOrderingProperties orderingProperties = extractOrderingProperties(message.getId());
            MessageOrderingProperties sessionData = localCache.get(sessionKey);
            if (sessionData == null) {
                sessionData = sessions.get(sessionKey);
            }
            MessageOrderingProperties lastOrderingProperties = sessionData == null
                    ? loadLastOrderingProperties(sessionKey)
                    : sessionData;

            verifyOrderingProperties(i, lastOrderingProperties, orderingProperties);
            localCache.put(sessionKey, orderingProperties);
        }
    }

    private SessionKey createSessionKey(GroupBatch batch, MessageId messageId) {
        return new SessionKey(batch.getBook(),
                batch.getSessionGroup(),
                messageId.getSessionAlias(),
                toCradleDirection(messageId.getDirection()));
    }

    private GroupKey createGroupKey(GroupBatch batch) {
        return new GroupKey(batch.getBook(),
                batch.getSessionGroup());
    }

    public static com.exactpro.cradle.Direction toCradleDirection(Direction direction) {
        switch (direction) {
            case INCOMING:
                return com.exactpro.cradle.Direction.FIRST;
            case OUTGOING:
                return com.exactpro.cradle.Direction.SECOND;
            default:
                throw new IllegalStateException("Unknown transport direction " + direction);
        }
    }

    public static MessageToStore toCradleMessage(String book, RawMessage rawMessage) throws CradleStorageException {
        MessageId messageId = rawMessage.getId();

        MessageToStoreBuilder builder = new MessageToStoreBuilder()
                .bookId(new BookId(book))
                .sessionAlias(messageId.getSessionAlias())
                .direction(toCradleDirection(messageId.getDirection()))
                .timestamp(messageId.getTimestamp())
                .sequence(messageId.getSequence())
                .protocol(rawMessage.getProtocol())
                .content(TransportUtilsKt.toByteArray(rawMessage.getBody()));

        rawMessage.getMetadata().forEach(builder::metadata);
        return builder.build();
    }
}
