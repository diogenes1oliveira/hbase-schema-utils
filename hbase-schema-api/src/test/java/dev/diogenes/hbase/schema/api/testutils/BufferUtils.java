package dev.diogenes.hbase.schema.api.testutils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class BufferUtils {
    private BufferUtils() {
        // utility class
    }

    public static ByteBuffer buffer(String s) {
        if (s == null) {
            return null;
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(bytes);
    }

    public static String string(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
