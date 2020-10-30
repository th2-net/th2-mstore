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

package com.exactpro.th2.mstore;

import java.util.List;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;

import com.exactpro.cradle.CradleManager;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageBatch;
import com.exactpro.th2.common.schema.message.MessageRouter;
import com.exactpro.th2.common.schema.message.QueueAttribute;
import com.exactpro.th2.store.common.utils.ProtoUtil;

public class RawMessageBatchStore extends AbstractMessageStore<RawMessageBatch> {
    private static final String[] ATTRIBUTES = Stream.of(QueueAttribute.SUBSCRIBE, QueueAttribute.RAW)
            .map(QueueAttribute::toString)
            .toArray(String[]::new);

    public RawMessageBatchStore(MessageRouter<RawMessageBatch> router, @NotNull CradleManager cradleManager) {
        super(router, cradleManager);
    }

    @Override
    protected String[] getAttributes() {
        return ATTRIBUTES;
    }

    @Override
    public void handle(RawMessageBatch batch) {
        try {
            List<RawMessage> messagesList = batch.getMessagesList();
            storeMessages(messagesList, ProtoUtil::toCradleMessage, getCradleManager().getStorage()::storeMessageBatch);
        } catch (Exception e) {
            logger.error("Can not store raw message batch: '{}'", batch, e);
        }
    }


}