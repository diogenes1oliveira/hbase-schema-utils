package hbase.schema.api.interfaces;

import org.jetbrains.annotations.Nullable;

public interface HBaseRowMapper<T> {
    byte @Nullable [] toRowKey(T object);

    @Nullable
    default Long toTimestamp(T object) {
        return null;
    }
}
