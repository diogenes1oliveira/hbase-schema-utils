package hbase.schema.api.interfaces;

import org.jetbrains.annotations.Nullable;

@FunctionalInterface
public interface LongMapper<T> {
    @Nullable Long toLong(@Nullable T value);
}
