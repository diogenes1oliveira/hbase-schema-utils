package hbase.schema.api.utils;

import java.nio.ByteBuffer;
import java.util.Comparator;

import static hbase.schema.api.utils.ByteBufferComparator.BYTE_BUFFER_COMPARATOR;

public class ByteBufferPrefixComparator implements Comparator<ByteBuffer> {
    private ByteBufferPrefixComparator() {
        // singleton
    }

    @Override
    public int compare(ByteBuffer b1, ByteBuffer b2) {
        if (b1.remaining() > b2.remaining()) {
            return -compare(b2, b1);
        }
        int prefixLength = b1.remaining();

        return BYTE_BUFFER_COMPARATOR.compare(b1, (ByteBuffer) b2.slice().limit(prefixLength));
    }

    public static final ByteBufferPrefixComparator BYTE_BUFFER_PREFIX_COMPARATOR = new ByteBufferPrefixComparator();
}
