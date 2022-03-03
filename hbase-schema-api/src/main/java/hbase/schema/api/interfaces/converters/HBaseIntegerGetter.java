package hbase.schema.api.interfaces.converters;

import org.jetbrains.annotations.Nullable;

/**
 * Interface to extract one {@code Integer} value from a Java object
 *
 * @param <T> object type
 */
public interface HBaseIntegerGetter<T> {
    @Nullable
    Integer getInteger(T obj);
}
