package hbase.schema.api.utils;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class ByteBufferComparator implements Comparator<ByteBuffer> {
    private ByteBufferComparator() {
        // singleton
    }

    @Override
    public int compare(ByteBuffer byteBuffer1, ByteBuffer byteBuffer2) {
        // manually because HBase tools don't like read-only buffers
        ByteBuffer buffer1 = byteBuffer1 == null ? ByteBuffer.allocate(0) : byteBuffer1.slice();
        ByteBuffer buffer2 = byteBuffer2 == null ? ByteBuffer.allocate(0) : byteBuffer2.slice();

        if (buffer1.remaining() > buffer2.remaining()) {
            return -compare(buffer2, buffer1);
        }

        while (buffer1.hasRemaining()) {
            byte b1 = buffer1.get();
            byte b2 = buffer2.get();
            if (b1 != b2) {
                return Byte.compare(b1, b2);
            }
        }

        return buffer2.hasRemaining() ? -1 : 0;
    }

    public static final ByteBufferComparator BYTE_BUFFER_COMPARATOR = new ByteBufferComparator();
}
