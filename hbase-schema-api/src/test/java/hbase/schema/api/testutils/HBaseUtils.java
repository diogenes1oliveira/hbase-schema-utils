package hbase.schema.api.testutils;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Function;

public final class HBaseUtils {
    private HBaseUtils() {
        // utility class
    }

    public static Function<String, byte[]> bytes() {
        return HBaseUtils::bytes;
    }

    public static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static Function<byte[], String> utf8() {
        return HBaseUtils::utf8;
    }

    public static String utf8(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static Function<NavigableMap<byte[], byte[]>, TreeMap<String, String>> utf8Map() {
        return bytesMap -> {
            TreeMap<String, String> result = new TreeMap<>();
            for (Map.Entry<byte[], byte[]> e : bytesMap.entrySet()) {
                String k = utf8(e.getKey());
                String v = utf8(e.getValue());
                result.put(k, v);
            }
            return result;
        };
    }

    public static LinkedHashMap<String, String> asStringMap(String... keysAndValues) {
        if (keysAndValues.length % 2 != 0) {
            int lastKeyIndex = (keysAndValues.length - 1) / 2;
            throw new IllegalArgumentException("Key #" + lastKeyIndex + " doesn't have a value");
        }
        LinkedHashMap<String, String> map = new LinkedHashMap<>();

        for (int i = 0; i < keysAndValues.length; i += 2) {
            String key = keysAndValues[i];
            String value = keysAndValues[i + 1];
            map.put(key, value);
        }

        return map;
    }
}
