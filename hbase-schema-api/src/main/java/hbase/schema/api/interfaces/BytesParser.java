package hbase.schema.api.interfaces;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface BytesParser<T> {
    default T parse(byte @NotNull [] bytes) {
        return this.parse(ByteBuffer.wrap(bytes));
    }

    default T parse(byte @NotNull [] bytes, int offset, int length) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);

        return this.parse(buffer);
    }

    T parse(@NotNull ByteBuffer buffer);

    BytesParser<ByteBuffer> IDENTITY = b -> b;
}
