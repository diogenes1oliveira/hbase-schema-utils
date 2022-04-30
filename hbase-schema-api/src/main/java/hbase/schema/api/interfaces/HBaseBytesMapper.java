package hbase.schema.api.interfaces;

import java.nio.ByteBuffer;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

@FunctionalInterface
public interface HBaseBytesMapper<T> {
    HBaseBytesMapper<Object> EMPTY = v -> emptyList();

    List<ByteBuffer> toBuffers(T value);

    static <T> HBaseBytesMapper<T> singleton(ByteBuffer buffer) {
        List<ByteBuffer> result = singletonList(buffer);
        return t -> result;
    }

    @SuppressWarnings("unchecked")
    static <T> HBaseBytesMapper<T> empty() {
        return (HBaseBytesMapper<T>) EMPTY;
    }
}
