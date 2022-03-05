package hbase.schema.api.converters;

import hbase.schema.api.interfaces.conversion.BytesMapConverter;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;

/**
 * Encodes the keys and values of a bytes map as UTF-8 strings
 */
public class Utf8BytesMapConverter implements BytesMapConverter<Map<String, String>> {
    private static final Utf8BytesMapConverter instance = new Utf8BytesMapConverter();

    /**
     * Converts the input map by encoding its keys and values as UTF-8
     *
     * @param map map of strings
     * @return map of bytes
     */
    @Override
    public NavigableMap<byte[], byte[]> toBytesMap(Map<String, String> map) {
        NavigableMap<byte[], byte[]> bytesMap = asBytesTreeMap();

        for (Map.Entry<String, String> entry : map.entrySet()) {
            bytesMap.put(entry.getKey().getBytes(StandardCharsets.UTF_8), entry.getValue().getBytes(StandardCharsets.UTF_8));
        }

        return bytesMap;
    }

    /**
     * Converts the result bytes map by decoding its keys and values as UTF-8
     *
     * @param bytesMap map of bytes
     * @return map of strings
     */
    @Override
    public Map<String, String> fromBytesMap(NavigableMap<byte[], byte[]> bytesMap) {
        Map<String, String> map = new HashMap<>();

        for (Map.Entry<byte[], byte[]> entry : bytesMap.entrySet()) {
            map.put(new String(entry.getKey(), StandardCharsets.UTF_8), new String(entry.getValue(), StandardCharsets.UTF_8));
        }

        return map;
    }

    /**
     * {@code Map.class}
     */
    @Override
    public Class<?> type() {
        return Map.class;
    }

    /**
     * An instance of this converter to aid in fluent APIs
     */
    public static Utf8BytesMapConverter utf8BytesMapConverter() {
        return instance;
    }


}
