package hbase.schema.api.interfaces.converters;

import org.jetbrains.annotations.Nullable;

public interface HBaseIntegerGetter<T> {
    @Nullable
    Integer getInteger(T obj);
}
