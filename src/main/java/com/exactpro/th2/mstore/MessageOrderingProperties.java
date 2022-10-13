/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

import java.time.Instant;
import java.util.Comparator;

import com.exactpro.th2.common.message.MessageUtils;
import org.jetbrains.annotations.NotNull;

import com.google.protobuf.Timestamp;

import static java.util.Objects.requireNonNull;

public class MessageOrderingProperties implements Comparable<MessageOrderingProperties> {

    private static final Comparator<Timestamp> TIMESTAMP_COMPARATOR = java.util.Comparator
                                              .comparingLong(Timestamp::getSeconds)
                                              .thenComparingInt(Timestamp::getNanos);

    public static final Comparator<MessageOrderingProperties> COMPARATOR = java.util.Comparator
                                              .comparingLong(MessageOrderingProperties::getSequence)
                                              .thenComparing(MessageOrderingProperties::getTimestamp, TIMESTAMP_COMPARATOR);

    public static final MessageOrderingProperties MIN_VALUE = new MessageOrderingProperties(Long.MIN_VALUE,
                                                                                            MessageUtils.toTimestamp(Instant.MIN));

    private final long sequence;
    private final Timestamp timestamp;

    public MessageOrderingProperties(long sequence, @NotNull Timestamp timestamp) {
        this.sequence = sequence;
        this.timestamp = requireNonNull(timestamp, "Timestamp cannot be null");
    }

    public boolean sequenceIsLessOrEquals(@NotNull MessageOrderingProperties other) {
        return sequence <= other.sequence;
    }

    public boolean timestampIsLess(@NotNull MessageOrderingProperties other) {
        return TIMESTAMP_COMPARATOR.compare(timestamp, other.timestamp) < 0;
    }

    public long getSequence() {
        return sequence;
    }

    @NotNull
    public Timestamp getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(@NotNull MessageOrderingProperties o) {
        return COMPARATOR.compare(this, o);
    }
}
