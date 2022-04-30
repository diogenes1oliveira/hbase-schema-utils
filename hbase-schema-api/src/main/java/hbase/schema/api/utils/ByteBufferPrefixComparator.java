package hbase.schema.api.utils;

import org.apache.hadoop.hbase.util.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class ByteBufferPrefixComparator implements Comparator<ByteBuffer> {
    @Override
    public int compare(ByteBuffer b1, ByteBuffer b2) {
        if (b1.remaining() > b2.remaining()) {
            return -compare(b2, b1);
        }
        int prefixLength = b1.remaining();
        return ByteBufferUtils.compareTo(b1, 0, prefixLength, b2, 0, prefixLength);
    }

    public static final ByteBufferPrefixComparator INSTANCE = new ByteBufferPrefixComparator();
}
