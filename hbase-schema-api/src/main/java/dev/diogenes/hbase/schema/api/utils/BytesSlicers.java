package dev.diogenes.hbase.schema.api.utils;

import dev.diogenes.hbase.schema.api.interfaces.BytesSlicer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class BytesSlicers {
    private BytesSlicers() {
        // utility class
    }

    public static BytesSlicer remainder() {
        return buffer -> {
            ByteBuffer result = buffer.slice();
            buffer.position(buffer.position() + result.remaining());
            return Optional.of(result);
        };
    }

    public static BytesSlicer fixed(int sliceSize) {
        return buffer -> {
            int remaining = buffer.remaining();
            if (remaining < sliceSize) {
                buffer.position(buffer.position() + buffer.remaining());
                return Optional.empty();
            }
            ByteBuffer result = (ByteBuffer) buffer.slice().limit(sliceSize);
            buffer.position(buffer.position() + sliceSize);
            return Optional.of(result);
        };
    }

    public static BytesSlicer split(byte separator) {
        return buffer -> {
            ByteBuffer result = buffer.slice();
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

    public static BytesSlicer split(byte[] separator) {
        return buffer -> {
            ByteBuffer result = buffer.slice();
            int size = 0;
            int i = 0;

            while (buffer.hasRemaining()) {
                byte b = buffer.get();
                if(b != separator[i]) {
                    i = 0;
                }
                if(b == separator[i]) {
                    i++;
                    if(i == separator.length) {

                    }
                }
            }

            result.limit(size);
            return Optional.of(result);
        };
    }

    public static BytesSlicer split(char separator) {
        return split((byte) separator);
    }

    public static List<ByteBuffer> toSlices(ByteBuffer buffer, List<BytesSlicer> slicers) {
        List<ByteBuffer> slices = new ArrayList<>();

        for (BytesSlicer slicer : slicers) {
            if (!buffer.hasRemaining()) {
                break;
            }
            ByteBuffer slice = slicer.slice(buffer).orElse(null);
            if (slice == null) {
                break;
            }
            slices.add(slice);
        }

        return slices;
    }
}
