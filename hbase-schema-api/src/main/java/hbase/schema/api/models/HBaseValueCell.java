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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import static hbase.schema.api.utils.HBaseSchemaConversions.removePrefix;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyNavigableMap;

/**
 * Data for a single HBase {@code long} cell
 */
public class HBaseValueCell implements Comparable<HBaseValueCell> {
    private final byte[] qualifier;
    private final byte[] value;
    private final Long timestamp;

    /**
     * @param qualifier {@link #getQualifier()}
     * @param value     {@link #getValue()}
     * @param timestamp {@link #getTimestamp()}
     */
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public HBaseValueCell(@JsonProperty("qualifier") byte[] qualifier,
                          @JsonProperty("value") byte @Nullable [] value,
                          @Nullable @JsonProperty("timestamp") Long timestamp) {
        this.qualifier = qualifier;
        this.value = value;
        this.timestamp = timestamp;
    }

    /**
     * @param qualifier {@link #getQualifier()}
     * @param value     {@link #getValue()}
     */
    public HBaseValueCell(byte[] qualifier, byte @Nullable [] value) {
        this(qualifier, value, null);
    }

    /**
     * HBase cell qualifier
     */
    @JsonProperty("qualifier")
    public byte[] getQualifier() {
        return qualifier;
    }

    /**
     * HBase generic cell value
     */
    @JsonProperty("value")
    public byte[] getValue() {
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
            builder = builder.append("value", Bytes.toStringBinary(value));
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

    public static List<HBaseValueCell> fromPrefixMap(byte[] prefix, Long timestamp, NavigableMap<byte[], byte[]> suffixMap) {
        if (prefix == null) {
            return emptyList();
        }
        List<HBaseValueCell> cells = new ArrayList<>();

        for (Map.Entry<byte[], byte[]> entry : suffixMap.entrySet()) {
            byte[] suffix = entry.getKey();
            byte[] qualifier = ArrayUtils.addAll(prefix, suffix);
            HBaseValueCell cell = new HBaseValueCell(qualifier, entry.getValue(), timestamp);
            cells.add(cell);
        }

        return cells;
    }

    public static NavigableMap<byte[], byte[]> withoutPrefix(byte[] prefix, Iterable<HBaseValueCell> cells) {
        if (prefix == null) {
            return emptyNavigableMap();
        }
        NavigableMap<byte[], byte[]> suffixMap = asBytesTreeMap();

        for (HBaseValueCell cell : cells) {
            byte[] value = cell.getValue();
            byte[] unprefixed = removePrefix(cell.getQualifier(), prefix);
            if (unprefixed == null || value == null) {
                continue;
            }
            suffixMap.put(unprefixed, value);
        }

        return suffixMap;
    }

    public static NavigableMap<byte[], byte[]> toCellsMap(Iterable<HBaseValueCell> cells) {
        return withoutPrefix(new byte[0], cells);
    }

}
