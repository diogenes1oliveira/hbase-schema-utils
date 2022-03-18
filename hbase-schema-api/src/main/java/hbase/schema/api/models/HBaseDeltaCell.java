package hbase.schema.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Data for a single HBase {@code long} cell
 */
public class HBaseDeltaCell implements Comparable<HBaseDeltaCell> {
    private final byte[] qualifier;
    private final Long value;

    /**
     * @param qualifier {@link #getQualifier()}
     * @param value     {@link #getValue()}
     */
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public HBaseDeltaCell(@JsonProperty("qualifier") byte[] qualifier,
                          @Nullable @JsonProperty("value") Long value) {
        this.qualifier = qualifier;
        this.value = value;
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
    public Long getValue() {
        return value;
    }

    @Override
    public String toString() {
        ToStringBuilder builder = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("qualifier", Bytes.toStringBinary(qualifier));

        if (value != null) {
            builder = builder.append("value", value);
        }

        return builder.build();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof HBaseDeltaCell)) {
            return false;
        }

        HBaseDeltaCell other = (HBaseDeltaCell) o;
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
    public int compareTo(@NotNull HBaseDeltaCell other) {
        return Bytes.BYTES_COMPARATOR.compare(this.qualifier, other.qualifier);
    }
}
