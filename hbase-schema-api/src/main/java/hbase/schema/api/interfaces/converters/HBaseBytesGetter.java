package hbase.schema.api.interfaces.converters;

import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

import static java.util.Optional.ofNullable;

public interface HBaseBytesGetter<T> {
    byte @Nullable [] getBytes(T obj);

    static <T, F> HBaseBytesGetter<T> bytesGetter(Function<T, F> getter, Function<F, byte[]> converter) {
        return obj -> ofNullable(getter.apply(obj)).map(converter).orElse(null);
    }
}
