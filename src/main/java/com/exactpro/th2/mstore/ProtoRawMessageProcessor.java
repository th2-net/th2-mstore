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
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.message.DeliveryMetadata;
import com.exactpro.th2.common.schema.message.ManualAckDeliveryCallback.Confirmation;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.common.schema.message.SubscriberMonitor;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.exactpro.th2.common.message.MessageUtils.toJson;
import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;
import static com.exactpro.th2.common.util.StorageUtils.toInstant;
import static com.google.protobuf.TextFormat.shortDebugString;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ProtoRawMessageProcessor extends AbstractMessageProcessor {
    protected static final String[] ATTRIBUTES = {QueueAttribute.SUBSCRIBE.getValue(), QueueAttribute.RAW.getValue()};
    private static final byte[] EMPTY_CONTENT = new byte[0];
    private static final Logger LOGGER = LoggerFactory.getLogger(ProtoRawMessageProcessor.class);
    // FIXME: migrate to the MessageGroupBatch router
    private final MessageRouter<RawMessageBatch> router;
    private SubscriberMonitor monitor;

    public ProtoRawMessageProcessor(
            @NotNull MessageRouter<RawMessageBatch> router,
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
            monitor = router.subscribeAllWithManualAck(this::process, ATTRIBUTES);
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
                LOGGER.error("Can not unsubscribe from queues", e);
            }
        }
        super.close();
    }


    void process(DeliveryMetadata deliveryMetadata, RawMessageBatch messageBatch, Confirmation confirmation) {
        try {
            List<RawMessage> messages = messageBatch.getMessagesList();
            if (messages.isEmpty()) {
                if (LOGGER.isWarnEnabled()) {
                    LOGGER.warn("Received empty batch {}", shortDebugString(messageBatch));
                }
                confirm(confirmation);
                return;
            }

            MessageID firstMessageId = messages.get(0).getMetadata().getId();
            GroupKey groupKey = createGroupKey(firstMessageId);

            if (!deliveryMetadata.isRedelivered()) {
                verifyBatch(groupKey, messages);
            }

            GroupedMessageBatchToStore groupedMessageBatchToStore = toCradleBatch(groupKey.sessionGroup, messages);

            for (RawMessage message : messages) {
                SessionKey sessionKey = createSessionKey(message.getMetadata().getId());
                MessageOrderingProperties sequenceToTimestamp = extractOrderingProperties(message.getMetadata().getId());
                sessions.computeIfAbsent(sessionKey, key -> sequenceToTimestamp);
            }
            groups.put(groupKey, groupedMessageBatchToStore.getLastTimestamp());

            if (deliveryMetadata.isRedelivered()) {
                persist(new ConsolidatedBatch(groupedMessageBatchToStore, confirmation));
            } else {
                storeMessages(groupedMessageBatchToStore, groupKey, confirmation);
            }
        } catch (Exception ex) {
            LOGGER.error("Cannot handle the batch of type {}, rejecting", messageBatch.getClass(), ex);
            reject(confirmation);
        }
    }

    protected MessageOrderingProperties extractOrderingProperties(MessageID messageID) {
        return new MessageOrderingProperties(
                messageID.getSequence(),
                messageID.getTimestamp()
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
    private GroupedMessageBatchToStore toCradleBatch(String group, List<RawMessage> messagesList) throws CradleStorageException {
        GroupedMessageBatchToStore batch = cradleStorage.getEntitiesFactory().groupedMessageBatch(group);
        for (RawMessage message : messagesList) {
            MessageToStore messageToStore = toCradleMessage(message);
            batch.addMessage(messageToStore);
        }
        return batch;
    }

    private void verifyBatch(GroupKey groupKey, List<RawMessage> messages) {
        Map<SessionKey, MessageOrderingProperties> localCache = new HashMap<>();
        SessionKey firstSessionKey = null;

        Instant lastGroupTimestamp = groups.get(groupKey);
        if (lastGroupTimestamp == null) {
            var bookId = new BookId(groupKey.bookName);
            lastGroupTimestamp = loadLastMessageTimestamp(bookId, groupKey.sessionGroup);
        }

        for (int i = 0; i < messages.size(); i++) {
            RawMessage message = messages.get(i);

            Timestamp messageTimestamp = message.getMetadata().getId().getTimestamp();
            if (compare(lastGroupTimestamp, messageTimestamp) > 0) {
                throw new IllegalArgumentException(format(
                        "Received grouped batch `%s` containing message with timestamp %s, but previously was %s",
                        groupKey.sessionGroup,
                        toJson(messageTimestamp),
                        lastGroupTimestamp
                ));
            }
            SessionKey sessionKey = createSessionKey(message.getMetadata().getId());
            if (firstSessionKey == null) {
                firstSessionKey = sessionKey;
            }

            if (!firstSessionKey.sessionGroup.equals(sessionKey.sessionGroup)) {
                throw new IllegalArgumentException(format(
                        "Delivery contains different session groups. Message [%d] - sequence %s; Message [%d] - sequence %s",
                        i - 1,
                        firstSessionKey,
                        i,
                        sessionKey
                ));
            }

            MessageOrderingProperties orderingProperties = extractOrderingProperties(message.getMetadata().getId());
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

    private SessionKey createSessionKey(MessageID messageId) {
        return new SessionKey(messageId.getBookName(),
                messageId.getConnectionId().getSessionGroup(),
                messageId.getConnectionId().getSessionAlias(),
                toCradleDirection(messageId.getDirection()));
    }

    private GroupKey createGroupKey(MessageID messageId) {
        return new GroupKey(messageId.getBookName(),
                messageId.getConnectionId().getSessionGroup(),
                messageId.getConnectionId().getSessionAlias());
    }

    public static MessageToStore toCradleMessage(RawMessage protoRawMessage) throws CradleStorageException {
        MessageID messageId = protoRawMessage.getMetadata().getId();
        ByteString body = protoRawMessage.getBody();
        MessageToStoreBuilder builder = new MessageToStoreBuilder()
                .bookId(new BookId(messageId.getBookName()))
                .sessionAlias(messageId.getConnectionId().getSessionAlias())
                .direction(toCradleDirection(messageId.getDirection()))
                .timestamp(toInstant(messageId.getTimestamp()))
                .sequence(messageId.getSequence())
                .protocol(protoRawMessage.getMetadata().getProtocol())
                .content(body.isEmpty() ? EMPTY_CONTENT : body.toByteArray());

        protoRawMessage.getMetadata().getPropertiesMap().forEach(builder::metadata);
        return builder.build();
    }

    private static int compare(Instant instant, Timestamp timestamp) {
        int compareResult = Long.compare(instant.getEpochSecond(), timestamp.getSeconds());
        return compareResult == 0 ? Integer.compare(instant.getNano(), timestamp.getNanos()) : compareResult;
    }
}
