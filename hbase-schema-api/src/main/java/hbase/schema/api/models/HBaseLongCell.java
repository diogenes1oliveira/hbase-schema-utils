package hbase.schema.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.NavigableMap;
import java.util.Objects;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

/**
 * Data for a single HBase {@code long} cell
 */
public class HBaseLongCell extends AbstractHBaseCell<Long> {
    private static final Long VALUE_DEFAULT = 0L;

    /**
     * @param qualifier {@link #getQualifier()}
     * @param value     {@link #getValue()}
     * @param timestamp {@link #getTimestamp()}
     */
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public HBaseLongCell(@JsonProperty("qualifier") byte[] qualifier,
                         @Nullable @JsonProperty("value") Long value,
                         @Nullable @JsonProperty("timestamp") Long timestamp) {
        super(qualifier, firstNonNull(value, VALUE_DEFAULT), timestamp);
    }

    /**
     * Stringifies the long value with {@link Objects#toString(Object)}
     */
    @Override
    public String toString(@Nullable Long value) {
        return Objects.toString(value);
    }

    /**
     * Compares just {@link #getQualifier()}
     */
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        } else if (!(o instanceof HBaseLongCell)) {
            return false;
        }

        HBaseLongCell other = (HBaseLongCell) o;
        return this.compareTo(other) == 0;
    }

    /**
     * Creates a map of {@code long} values given the cell objects
     *
     * @param longCells input cell objects
     * @return map of (qualifier -> {@code Long} value)
     */
    public static NavigableMap<byte[], Long> longCellsToMap(Collection<HBaseLongCell> longCells) {
        NavigableMap<byte[], Long> result = asBytesTreeMap();

        for (HBaseLongCell cell : longCells) {
            result.put(cell.getQualifier(), cell.getValue());
        }

        return result;
    }

}
