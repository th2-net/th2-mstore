/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.messagestore;

import static com.exactpro.cradle.messages.StoredMessageBatch.MAX_MESSAGES_COUNT;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import org.jetbrains.annotations.NotNull;

import com.exactpro.cradle.CradleManager;
import com.exactpro.cradle.messages.MessageToStore;
import com.exactpro.cradle.messages.StoredMessageBatch;
import com.exactpro.cradle.utils.CradleStorageException;
import com.exactpro.th2.schema.message.MessageRouter;
import com.exactpro.th2.store.common.AbstractStorage;
import com.google.protobuf.MessageLite;

public abstract class AbstractMessageStore<T> extends AbstractStorage<T> {
    public AbstractMessageStore(MessageRouter<T> router, @NotNull CradleManager cradleManager) {
        super(router, cradleManager);
    }

    protected  <T extends MessageLite> void storeMessages(List<T> messagesList, Function<T, MessageToStore> convertToMessageToStore,
            CradleStoredMessageBatchFunction cradleStoredMessageBatchFunction) throws CradleStorageException, IOException {
        if (messagesList.isEmpty()) {
            logger.warn("Empty batch has been received"); //FIXME: need identify
            return;
        }

        logger.debug("Process {} messages started, max {}", messagesList.size(), MAX_MESSAGES_COUNT);
        for(int from = 0; from < messagesList.size(); from += MAX_MESSAGES_COUNT) {
            List<T> storedMessages = messagesList.subList(from, Math.min(from + MAX_MESSAGES_COUNT, messagesList.size()));

            StoredMessageBatch storedMessageBatch = new StoredMessageBatch();
            for (int index = 0; index < storedMessages.size(); index++) {
                T message = storedMessages.get(index);
                storedMessageBatch.addMessage(convertToMessageToStore.apply(message));
            }
            cradleStoredMessageBatchFunction.store(storedMessageBatch);
            logger.debug("Message Batch stored: stream '{}', direction '{}', id '{}', size '{}'",
                    storedMessageBatch.getStreamName(), storedMessageBatch.getId().getDirection(), storedMessageBatch.getId().getIndex(), storedMessageBatch.getMessageCount());
        }
        logger.debug("Process {} messages finished", messagesList.size());
    }

    @FunctionalInterface
    protected interface CradleStoredMessageBatchFunction {
        void store(StoredMessageBatch storedMessageBatch) throws IOException;
    }
}
