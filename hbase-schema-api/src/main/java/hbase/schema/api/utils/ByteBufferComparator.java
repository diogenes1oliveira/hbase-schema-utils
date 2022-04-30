package hbase.schema.api.utils;

import org.apache.hadoop.hbase.util.ByteBufferUtils;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class ByteBufferComparator implements Comparator<ByteBuffer> {
    @Override
    public int compare(ByteBuffer b1, ByteBuffer b2) {
        return ByteBufferUtils.compareTo(b1, 0, b1.remaining(), b2, 0, b2.remaining());
    }

    public static final ByteBufferComparator INSTANCE = new ByteBufferComparator();
}
