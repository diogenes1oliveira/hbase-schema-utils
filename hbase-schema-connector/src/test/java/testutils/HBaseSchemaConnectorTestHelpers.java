package testutils;

import hbase.schema.api.interfaces.converters.HBaseBytesMapSetter;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.NavigableMap;
import java.util.TreeMap;

import static hbase.schema.api.interfaces.converters.HBaseBytesMapSetter.bytesMapSetter;
import static hbase.schema.api.utils.HBaseSchemaConversions.utf8FromBytes;

public final class HBaseSchemaConnectorTestHelpers {
    private HBaseSchemaConnectorTestHelpers() {
        // utility class
    }

    public static TreeMap<String, String> bytesToStringMap(NavigableMap<byte[], byte[]> cells) {
        HBaseBytesMapSetter<TreeMap<String, String>> setter = bytesMapSetter(TreeMap::putAll, utf8FromBytes(), utf8FromBytes());

        TreeMap<String, String> result = new TreeMap<>();
        setter.setFromBytes(result, cells);
        return result;
    }

    public static TreeMap<String, Long> bytesToLongMap(NavigableMap<byte[], byte[]> cells) {
        HBaseBytesMapSetter<TreeMap<String, Long>> setter = bytesMapSetter(TreeMap::putAll, utf8FromBytes(), Bytes::toLong);

        TreeMap<String, Long> result = new TreeMap<>();
        setter.setFromBytes(result, cells);
        return result;
    }
}
