package hbase.schema.api.interfaces;

import org.jetbrains.annotations.Nullable;

import java.util.NavigableMap;

public interface HBaseMutationSchema<T> {
    byte @Nullable [] buildRowKey(T object);

    @Nullable
    Long buildTimestamp(T object);

    @Nullable
    default Long buildTimestamp(T object, byte[] qualifier) {
        return buildTimestamp(object);
    }

    NavigableMap<byte[], byte[]> buildCellValues(T object);

    NavigableMap<byte[], Long> buildCellIncrements(T object);
}
