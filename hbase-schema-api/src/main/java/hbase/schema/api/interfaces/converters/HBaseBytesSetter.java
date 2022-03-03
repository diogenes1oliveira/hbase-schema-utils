package hbase.schema.api.interfaces.converters;

import java.util.function.BiConsumer;
import java.util.function.Function;

import static java.util.Optional.ofNullable;

@FunctionalInterface
public interface HBaseBytesSetter<T> {
    void setFromBytes(T obj, byte[] bytes);

    static <T> HBaseBytesSetter<T> dummy() {
        return ((obj, bytes) -> {

        });
    }

    static <T, F> HBaseBytesSetter<T> bytesSetter(BiConsumer<T, F> setter, Function<byte[], F> converter) {
        return ((obj, bytes) ->
                ofNullable(bytes).map(converter).ifPresent(f -> setter.accept(obj, f))
        );
    }
}
