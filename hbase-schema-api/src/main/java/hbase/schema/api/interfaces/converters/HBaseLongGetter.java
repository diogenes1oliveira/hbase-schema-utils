package hbase.schema.api.interfaces.converters;

import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

import static java.util.Optional.ofNullable;

public interface HBaseLongGetter<T> {
    @Nullable
    Long getLong(T obj);

    static <T, F> HBaseLongGetter<T> longGetter(Function<T, F> getter, Function<F, Long> converter) {
        return obj -> ofNullable(getter.apply(obj)).map(converter).orElse(null);
    }
}
