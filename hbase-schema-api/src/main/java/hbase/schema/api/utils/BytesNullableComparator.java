package hbase.schema.api.utils;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Comparator;

public class BytesNullableComparator implements Comparator<byte[]> {
    public static final BytesNullableComparator BYTES_NULLABLE_COMPARATOR = new BytesNullableComparator();

    private BytesNullableComparator() {
        // singleton
    }

    @Override
    public int compare(byte[] b1, byte[] b2) {
        if (isEmpty(b1)) {
            return isEmpty(b2) ? 0 : -1;
        } else if (isEmpty(b2)) {
            return 1;
        } else {
            return Bytes.BYTES_COMPARATOR.compare(b1, b2);
        }
    }

    public static boolean isEmpty(byte[] bytes) {
        return bytes == null || bytes.length == 0;
    }
}
