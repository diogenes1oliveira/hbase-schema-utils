package hbase.schema.api.models;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class HBaseRowRange {
    private final byte[] start;
    private final byte[] stop;

    public HBaseRowRange(byte @Nullable [] start, byte @Nullable [] stop) {
        this.start = start;
        this.stop = stop;
    }

    public HBaseRowRange(byte @NotNull [] prefix) {
        this(prefix, Bytes.unsignedCopyAndIncrement(prefix));
    }

    public byte @Nullable [] start() {
        return start;
    }

    public byte @Nullable [] stop() {
        return stop;
    }

    public String toString() {
        return "[ " + Bytes.toStringBinary(start) + " , " + Bytes.toStringBinary(stop) + " )";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (!(o instanceof HBaseRowRange)) {
            return false;
        }

        HBaseRowRange other = (HBaseRowRange) o;

        return new EqualsBuilder()
                .append(this.start, other.start)
                .append(this.stop, other.stop)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(start)
                .append(stop)
                .toHashCode();
    }
}
