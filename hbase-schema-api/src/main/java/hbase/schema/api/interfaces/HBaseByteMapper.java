package hbase.schema.api.interfaces;

import java.nio.ByteBuffer;
import java.util.function.Function;

@FunctionalInterface
public interface HBaseByteMapper<T> {
    ByteBuffer toBuffer(T value);

    default HBaseByteMapper<T> andThen(Function<ByteBuffer, ByteBuffer> mapper) {
        return value -> {
            ByteBuffer buffer = this.toBuffer(value);
            if (buffer == null) {
                return null;
            } else {
                return mapper.apply(buffer);
            }
        };
    }

    static <T> HBaseByteMapper<T> singleton(ByteBuffer buffer) {
        return t -> buffer;
    }
}
