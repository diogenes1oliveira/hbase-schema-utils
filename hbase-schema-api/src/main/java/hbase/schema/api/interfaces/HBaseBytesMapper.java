package hbase.schema.api.interfaces;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

@FunctionalInterface
public interface HBaseBytesMapper<T> {
    HBaseBytesMapper<Object> EMPTY = v -> emptyList();

    List<ByteBuffer> toBuffers(T value);

    default HBaseBytesMapper<T> andThen(Function<ByteBuffer, ByteBuffer> mapper) {
        return value -> toBuffers(value).stream().map(mapper).collect(toList());
    }

    static <T> HBaseBytesMapper<T> singleton(ByteBuffer buffer) {
        List<ByteBuffer> result = singletonList(buffer);
        return t -> result;
    }

    @SuppressWarnings("unchecked")
    static <T> HBaseBytesMapper<T> empty() {
        return (HBaseBytesMapper<T>) EMPTY;
    }
}
