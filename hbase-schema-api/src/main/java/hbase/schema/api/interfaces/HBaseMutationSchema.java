package hbase.schema.api.interfaces;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;

import java.util.Map;
import java.util.NavigableMap;

public interface HBaseMutationSchema<T> {
    @Nullable
    byte[] buildRowKey(T object);

    @Nullable
    Long buildTimestamp(T object);

    @Nullable
    default Long buildTimestamp(T object, byte[] qualifier) {
        return buildTimestamp(object);
    }

    NavigableMap<byte[], byte[]> buildCellValues(T object);

    NavigableMap<byte[], Long> buildCellIncrements(T object);

    @Nullable
    default Put toPut(T object, byte[] family) {
        byte[] rowKey = buildRowKey(object);
        Long timestamp = buildTimestamp(object);

        if (rowKey == null || timestamp == null) {
            return null;
        }
        NavigableMap<byte[], byte[]> cellValues = buildCellValues(object);
        if (cellValues.isEmpty()) {
            return null;
        }

        Put put = new Put(rowKey);

        for (Map.Entry<byte[], byte[]> entry : cellValues.entrySet()) {
            put.addColumn(family, entry.getKey(), timestamp, entry.getValue());
        }

        return put;
    }

    @Nullable
    default Increment toIncrement(T object, byte[] family) {
        byte[] rowKey = buildRowKey(object);
        Long timestamp = buildTimestamp(object);

        if (rowKey == null || timestamp == null) {
            return null;
        }
        NavigableMap<byte[], Long> longValues = buildCellIncrements(object);
        if (longValues.isEmpty()) {
            return null;
        }

        Increment increment = new Increment(rowKey);

        for (Map.Entry<byte[], Long> entry : longValues.entrySet()) {
            increment.addColumn(family, entry.getKey(), entry.getValue());
        }

        return increment;
    }
}
