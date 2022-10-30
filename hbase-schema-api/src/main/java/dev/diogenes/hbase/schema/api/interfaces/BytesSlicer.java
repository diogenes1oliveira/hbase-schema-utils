package dev.diogenes.hbase.schema.api.interfaces;

import java.nio.ByteBuffer;
import java.util.Optional;

@FunctionalInterface
public interface BytesSlicer {
    Optional<ByteBuffer> slice(ByteBuffer buffer);

    static BytesSlicer full() {
        return buffer -> {
            ByteBuffer result = (ByteBuffer) buffer.slice();
            buffer.position(buffer.position() + result.remaining());
            return Optional.of(result);
        };
    }

    static BytesSlicer fixed(int sliceSize) {
        return buffer -> {
            if (buffer.remaining() < sliceSize) {
                return Optional.empty();
            }
            ByteBuffer result = (ByteBuffer) buffer.slice(0, sliceSize);
            buffer.position(buffer.position() + sliceSize);
            return Optional.of(result);
        };
    }

    static BytesSlicer split(byte separator) {
        return buffer -> {
            ByteBuffer result = (ByteBuffer) buffer.slice();
            int size = 0;

            while (buffer.hasRemaining()) {
                byte b = buffer.get();
                if (b != separator) {
                    size++;
                } else {
                    break;
                }
            }

            result.limit(size);
            return Optional.of(result);
        };
    }

}
