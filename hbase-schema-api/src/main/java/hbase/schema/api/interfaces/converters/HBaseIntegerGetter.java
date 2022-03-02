package hbase.schema.api.interfaces.converters;

import edu.umd.cs.findbugs.annotations.Nullable;

public interface HBaseIntegerGetter<T> {
    @Nullable
    Integer getInteger(T obj);
}
