package hbase.schema.api.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
     * @param qualifier {@link #getQualifier()}
     * @param value     {@link #getValue()}
     */
    public HBaseValueCell(byte[] qualifier, byte @Nullable [] value) {
        this(qualifier, value, null);
    }

    public static NavigableMap<byte[], byte[]> withoutPrefix(byte[] prefix, Collection<HBaseValueCell> cells) {
        NavigableMap<byte[], byte[]> map = asBytesTreeMap();

        Iterator<HBaseValueCell> it = cells.iterator();
        while (it.hasNext()) {
            HBaseValueCell cell = it.next();
            if (cell.hasPrefix(prefix)) {
                byte[] qualifier = cell.getQualifier();
                byte[] unprefixed = Arrays.copyOfRange(qualifier, prefix.length, qualifier.length);
                byte[] value = cell.getValue();
                it.remove();
                map.put(unprefixed, value);
            }
        }
        return map;
    }

    public static List<HBaseValueCell> fromPrefixMap(byte[] prefix, Long timestamp, NavigableMap<byte[], byte[]> prefixMap) {
        List<HBaseValueCell> cells = new ArrayList<>();

        for (Map.Entry<byte[], byte[]> entry : prefixMap.entrySet()) {
            byte[] qualifier = ArrayUtils.addAll(prefix, entry.getKey());
            byte[] value = entry.getValue();
            HBaseValueCell cell = new HBaseValueCell(qualifier, value, timestamp);
            cells.add(cell);
        }

        return cells;
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
