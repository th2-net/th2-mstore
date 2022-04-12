/*
 * Copyright 2022 Exactpro (Exactpro Systems Limited)
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

import java.time.Instant;
import java.util.Comparator;

import org.jetbrains.annotations.NotNull;

import com.google.protobuf.Timestamp;

import static com.exactpro.th2.common.message.MessageUtils.toTimestamp;
import static java.util.Objects.requireNonNull;

public class SequenceToTimestamp {
    // TODO move to common? the same is needed for books/pages in estore
    private static final Comparator<Timestamp> TIMESTAMP_COMPARATOR = Comparator
            .comparingLong(Timestamp::getSeconds)
            .thenComparingInt(Timestamp::getNanos);

    public static final Comparator<SequenceToTimestamp> SEQUENCE_TO_TIMESTAMP_COMPARATOR = Comparator
            .comparingLong(SequenceToTimestamp::getSequence)
            .thenComparing((s1, s2) -> TIMESTAMP_COMPARATOR.compare(s1.getTimestamp(), s2.getTimestamp()));

    private final long sequence;
    private final Timestamp timestamp;

    public static final SequenceToTimestamp MIN = new SequenceToTimestamp();

    private SequenceToTimestamp() {
        this(Long.MIN_VALUE, toTimestamp(Instant.MIN));
    }

    public SequenceToTimestamp(long sequence, @NotNull Timestamp timestamp) {
        this.sequence = sequence;
        this.timestamp = requireNonNull(timestamp, "Timestamp cannot be null");
    }

    public boolean sequenceIsLessOrEquals(@NotNull SequenceToTimestamp another) {
        requireNonNull(another, "Another sequence to timestamp cannot be null");
        return sequence <= another.sequence;
    }

    public boolean timestampIsLess(@NotNull SequenceToTimestamp another) {
        requireNonNull(another, "Another sequence to timestamp cannot be null");
        return TIMESTAMP_COMPARATOR.compare(timestamp, another.timestamp) < 0;
    }

    public long getSequence() {
        return sequence;
    }

    @NotNull
    public Timestamp getTimestamp() {
        return timestamp;
    }
}
