package hbase.schema.api.interfaces;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface HBaseByteMapper<T> {
    ByteBuffer toBuffer(T value);

    default HBaseByteMapper<T> crop(int size) {
        return value -> (ByteBuffer) this.toBuffer(value).limit(size);
    }

    static <T> HBaseByteMapper<T> singleton(ByteBuffer buffer) {
        return t -> buffer;
    }

    static <T> HBaseByteMapper<T> singleton(byte[] bytes) {
        return singleton(ByteBuffer.wrap(bytes).asReadOnlyBuffer());
    }
}
