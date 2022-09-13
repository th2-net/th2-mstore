/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.Direction;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredGroupMessageBatch;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageBatch;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.google.protobuf.TextFormat;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static com.exactpro.th2.common.util.StorageUtils.toCradleDirection;

@Deprecated(since = "5.0.0")
public class MessageBatchStore extends AbstractMessageStore<MessageBatch, Message> {
    private static final Logger logger = LoggerFactory.getLogger(MessageBatchStore.class);
    private static final String[] ATTRIBUTES = Stream.of(QueueAttribute.SUBSCRIBE, QueueAttribute.PARSED)
            .map(QueueAttribute::toString)
            .toArray(String[]::new);

    public MessageBatchStore(
            MessageRouter<MessageBatch> router,
            @NotNull CradleManager cradleManager,
            @NotNull Configuration configuration
    ) {
        super(router, cradleManager, configuration);
    }


    @Override
    public void handle(MessageBatch messageBatch) {
        try {
            List<Message> messages = getMessages(messageBatch);
            if (messages.isEmpty()) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Empty batch has been received {}", shortDebugString(messageBatch));
                }
                return;
            }
            Message firstMessage = messages.get(0);
            if (logger.isWarnEnabled()) {
                logger.warn("Ignoring parsed batch for {}. Parsed batches are deprecated", createSessionKey(firstMessage));
            }
        } catch (Exception ex) {
            if (logger.isErrorEnabled()) {
                logger.error("Cannot handle the batch of type {} message id {}", messageBatch.getClass(), shortDebugString(messageBatch), ex);
            }
        }
    }


    @Override
    protected MessageToStore convert(Message originalMessage) {
        return ProtoUtil.toCradleMessage(originalMessage);
    }

    @Override
    protected CompletableFuture<Void> store(StoredGroupMessageBatch messageBatch, String sessionGroup) {
        logger.warn("Storing parsed messages has been deprecated, doing nothing");
        return new CompletableFuture<>();
    }

    @Override
    protected MessageOrderingProperties extractSequenceToTimestamp(Message message) {
        return new MessageOrderingProperties(
                message.getMetadata().getId().getSequence(),
                message.getMetadata().getTimestamp()
        );
    }

    @Override
    protected SessionKey createSessionKey(Message message) {
        MessageID messageID = message.getMetadata().getId();
        String sessionAlias = messageID.getConnectionId().getSessionAlias();
        Direction direction = toCradleDirection(messageID.getDirection());
        String sessionGroup = messageID.getConnectionId().getSessionGroup();
        sessionGroup = (sessionGroup == null || sessionGroup.isEmpty()) ? sessionAlias : sessionGroup;
        return new SessionKey(sessionAlias, direction, sessionGroup);
    }

    @Override
    protected String[] getAttributes() {
        return ATTRIBUTES;
    }

    @Override
    protected List<Message> getMessages(MessageBatch delivery) {
        return delivery.getMessagesList();
    }

    @Override
    protected String shortDebugString(MessageBatch batch) {
        return TextFormat.shortDebugString(batch);
    }
}
