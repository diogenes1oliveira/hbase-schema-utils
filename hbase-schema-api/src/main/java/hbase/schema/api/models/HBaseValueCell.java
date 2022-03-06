package hbase.schema.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.NavigableMap;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;

/**
 * Data for a single HBase {@code byte[]} cell
 */
public class HBaseValueCell extends AbstractHBaseCell<byte[]> {
    /**
     * @param qualifier {@link #getQualifier()}
     * @param value     {@link #getValue()}
     * @param timestamp {@link #getTimestamp()}
     */
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public HBaseValueCell(@JsonProperty("qualifier") byte[] qualifier,
                          @JsonProperty("value") byte @Nullable [] value,
                          @Nullable @JsonProperty("timestamp") Long timestamp) {
        super(qualifier, value, timestamp);
    }

    /**
     * Creates a map of {@code byte[]} values given the cell objects
     *
     * @param valueCells input cell objects
     * @return map of (qualifier -> {@code byte[]} value)
     */
    public static NavigableMap<byte[], byte[]> valueCellsToMap(Collection<HBaseValueCell> valueCells) {
        NavigableMap<byte[], byte[]> result = asBytesTreeMap();

        for (HBaseValueCell cell : valueCells) {
            result.put(cell.getQualifier(), cell.getValue());
        }

        return result;
    }

    /**
     * Stringifies the {@code byte[]} value with {@link Bytes#toStringBinary}
     */
    @Override
    protected String toString(byte @Nullable [] value) {
        return Bytes.toStringBinary(value);
    }

}
