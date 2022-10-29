package hbase.schema.api.interfaces;

import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

import static java.util.Optional.ofNullable;

@FunctionalInterface
public interface BytesMapper<T> {
    byte @Nullable [] toBytes(@Nullable T value);

    default @Nullable ByteBuffer toBuffer(@Nullable T value) {
        return ofNullable(value)
                .map(this::toBytes)
                .map(ByteBuffer::wrap)
                .orElse(null);
    }
}
