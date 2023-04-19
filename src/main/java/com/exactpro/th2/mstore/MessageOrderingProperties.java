/*
 * Copyright 2022-2023 Exactpro (Exactpro Systems Limited)
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

import com.google.protobuf.Timestamp;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Comparator;
import java.util.Objects;

public class MessageOrderingProperties implements Comparable<MessageOrderingProperties> {

    private static final Comparator<MessageOrderingProperties> TIMESTAMP_COMPARATOR = java.util.Comparator
            .comparingLong(MessageOrderingProperties::getEpochSecond)
            .thenComparingInt(MessageOrderingProperties::getNano);
    public static final Comparator<MessageOrderingProperties> COMPARATOR = java.util.Comparator
            .comparingLong(MessageOrderingProperties::getSequence)
            .thenComparing(TIMESTAMP_COMPARATOR);

    public static final MessageOrderingProperties MIN_VALUE = new MessageOrderingProperties(
            Long.MIN_VALUE,
            Instant.MIN.getEpochSecond(),
            Instant.MIN.getNano()
    );

    private final long sequence;
    private final long epochSecond;
    private final int nano;

    private MessageOrderingProperties(long sequence, long epochSecond, int nano) {
        this.sequence = sequence;
        this.epochSecond = epochSecond;
        this.nano = nano;
    }

    public MessageOrderingProperties(long sequence, Timestamp timestamp) {
        this(
                sequence,
                Objects.requireNonNull(timestamp, "'timestamp' can't be null").getSeconds(),
                Objects.requireNonNull(timestamp, "'timestamp' can't be null").getNanos()
        );
    }

    public MessageOrderingProperties(long sequence, Instant timestamp) {
        this(
                sequence,
                Objects.requireNonNull(timestamp, "'timestamp' can't be null").getEpochSecond(),
                Objects.requireNonNull(timestamp, "'timestamp' can't be null").getNano()
        );
    }

    public boolean sequenceIsLessOrEquals(@NotNull MessageOrderingProperties other) {
        return sequence <= other.sequence;
    }

    public boolean timestampIsLess(@NotNull MessageOrderingProperties other) {
        return TIMESTAMP_COMPARATOR.compare(this, other) < 0;
    }

    public long getSequence() {
        return sequence;
    }

    public long getEpochSecond() {
        return epochSecond;
    }

    public int getNano() {
        return nano;
    }

    @Override
    public int compareTo(@NotNull MessageOrderingProperties o) {
        return COMPARATOR.compare(this, o);
    }
}