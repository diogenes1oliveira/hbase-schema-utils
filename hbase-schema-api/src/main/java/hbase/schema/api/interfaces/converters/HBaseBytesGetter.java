package hbase.schema.api.interfaces.converters;

import edu.umd.cs.findbugs.annotations.Nullable;

public interface HBaseBytesGetter<T> {
    @Nullable
    byte[] getBytes(T obj);
}
