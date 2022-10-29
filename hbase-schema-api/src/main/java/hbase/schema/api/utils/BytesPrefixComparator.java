package hbase.schema.api.utils;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Comparator;

public class BytesPrefixComparator implements Comparator<byte[]> {
    private BytesPrefixComparator() {
        // singleton
    }

    @Override
    public int compare(byte[] b1, byte[] b2) {
        if (b1.length > b2.length) {
            return -compare(b2, b1);
        }
        int prefixLength = b1.length;
        return Bytes.compareTo(b1, 0, prefixLength, b2, 0, prefixLength);
    }

    public static final BytesPrefixComparator BYTES_PREFIX_COMPARATOR = new BytesPrefixComparator();
}
