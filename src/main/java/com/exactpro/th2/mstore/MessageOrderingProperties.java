package com.exactpro.th2.mstore;

import com.exactpro.th2.common.message.MessageUtils;
import com.google.protobuf.Timestamp;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Comparator;

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