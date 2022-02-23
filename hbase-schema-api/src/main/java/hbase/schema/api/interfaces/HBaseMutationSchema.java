package hbase.schema.api.interfaces;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;

import java.util.Map;
import java.util.NavigableMap;

public interface HBaseMutationSchema<T> {
    byte[] buildRowKey(T object);

    long buildTimestamp(T object);

    default long buildTimestamp(T object, byte[] qualifier) {
        return buildTimestamp(object);
    }

    NavigableMap<byte[], byte[]> buildCellValues(T object);

    NavigableMap<byte[], Long> buildCellIncrements(T object);

    @Nullable
    default Put toPut(T object, byte[] family) {
        NavigableMap<byte[], byte[]> cellValues = buildCellValues(object);
        if (cellValues.isEmpty()) {
            return null;
        }

        byte[] rowKey = buildRowKey(object);
        long timestamp = buildTimestamp(object);
        Put put = new Put(rowKey);

        for (Map.Entry<byte[], byte[]> entry : cellValues.entrySet()) {
            put.addColumn(family, entry.getKey(), timestamp, entry.getValue());
        }

        return put;
    }

    @Nullable
    default Increment toIncrement(T object, byte[] family) {
        NavigableMap<byte[], Long> longValues = buildCellIncrements(object);
        if (longValues.isEmpty()) {
            return null;
        }

        byte[] rowKey = buildRowKey(object);
        Increment increment = new Increment(rowKey);

        for (Map.Entry<byte[], Long> entry : longValues.entrySet()) {
            increment.addColumn(family, entry.getKey(), entry.getValue());
        }

        return increment;
    }
}
