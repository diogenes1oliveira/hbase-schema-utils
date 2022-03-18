package hbase.schema.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Data for a single HBase {@code long} cell
 */
public class HBaseValueCell implements Comparable<HBaseValueCell> {
    private final ByteBuffer qualifier;
    private final ByteBuffer value;
    private final Long timestamp;

    /**
     * @param qualifier {@link #getQualifier()}
     * @param value     {@link #getValue()}
     * @param timestamp {@link #getTimestamp()}
     */
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public HBaseValueCell(@JsonProperty("qualifier") ByteBuffer qualifier,
                          @JsonProperty("value") @Nullable ByteBuffer value,
                          @Nullable @JsonProperty("timestamp") Long timestamp) {
        this.qualifier = qualifier;
        this.value = value;
        this.timestamp = timestamp;
    }

    /**
     * @param qualifier {@link #getQualifier()}
     * @param value     {@link #getValue()}
     */
    public HBaseValueCell(ByteBuffer qualifier, byte @Nullable [] value) {
        this(qualifier, value, null);
    }

    /**
     * HBase cell qualifier
     */
    @JsonProperty("qualifier")
    public ByteBuffer getQualifier() {
        return qualifier;
    }

    /**
     * HBase generic cell value
     */
    @JsonProperty("value")
    public ByteBuffer getValue() {
        return value;
    }

    /**
     * HBase cell timestamp in milliseconds
     */
    @JsonProperty("timestamp")
    @Nullable
    public Long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("qualifier", Bytes.toStringBinary(qualifier));

        if (value != null) {
            builder = builder.append("value", value);
        }

        if (timestamp != null) {
            Instant instant = Instant.ofEpochMilli(timestamp);
            builder = builder.append("timestamp", instant);
        }

        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof HBaseValueCell)) {
            return false;
        }

        HBaseValueCell other = (HBaseValueCell) o;
        return new EqualsBuilder()
                .append(this.qualifier, other.qualifier)
                .append(this.value, other.value)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(3, 37)
                .append(qualifier)
                .append(value)
                .toHashCode();
    }

    @Override
    public int compareTo(@NotNull HBaseValueCell other) {
        return Bytes.BYTES_COMPARATOR.compare(this.qualifier, other.qualifier);
    }

}
