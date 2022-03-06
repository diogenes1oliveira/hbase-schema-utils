package hbase.schema.api.models;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

/**
 * Data for a single HBase cell with a generic type
 *
 * @param <T> type of the cell value
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractHBaseCell<T> implements Comparable<AbstractHBaseCell> {
    private final byte[] qualifier;
    private final T value;
    private final Long timestamp;

    /**
     * @param qualifier {@link #getQualifier()}
     * @param value     {@link #getValue()}
     * @param timestamp {@link #getTimestamp()}
     */
    protected AbstractHBaseCell(byte[] qualifier, @Nullable T value, @Nullable Long timestamp) {
        this.qualifier = qualifier;
        this.value = value;
        this.timestamp = timestamp;
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
    @Nullable
    public T getValue() {
        return value;
    }

    /**
     * HBase cell timestamp in milliseconds
     * <p>
     * Defaults to -1
     */
    @JsonProperty("timestamp")
    @Nullable
    public Long getTimestamp() {
        return timestamp;
    }

    /**
     * Compares just {@link #getQualifier()}
     */
    @Override
    public int compareTo(@NotNull AbstractHBaseCell other) {
        return Bytes.compareTo(this.qualifier, other.qualifier);
    }

    /**
     * Compares just {@link #getQualifier()}
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof AbstractHBaseCell)) {
            return false;
        }

        AbstractHBaseCell other = (AbstractHBaseCell) o;
        return this.compareTo(other) == 0;
    }

    /**
     * Hashes just {@link #getQualifier()}
     */
    @Override
    public int hashCode() {
        return Bytes.hashCode(qualifier);
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("qualifier", Bytes.toStringBinary(qualifier))
                .append("value", toString(value));

        if (timestamp != null) {
            Instant instant = Instant.ofEpochMilli(timestamp);
            builder = builder.append("timestamp", instant);
        }

        return builder.build();
    }

    /**
     * Stringifies the cell value
     */
    protected abstract String toString(@Nullable T value);
}
