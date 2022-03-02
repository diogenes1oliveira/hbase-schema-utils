package hbase.schema.api.interfaces.converters;

import edu.umd.cs.findbugs.annotations.Nullable;

public interface HBaseLongGetter<T> {
    @Nullable
    Long getLong(T obj);
}
