package hbase.schema.api.utils;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Function;

/**
 * Generic utility aid methods
 */
public final class HBaseSchemaConversions {
    private static final Long LONG_ONE = 1L;

    private HBaseSchemaConversions() {
        // utility class
    }

    /**
     * Encodes the string as UTF-8 bytes
     */
    public static byte[] utf8ToBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Function to encode the string as UTF-8 bytes
     */
    public static Function<String, byte[]> utf8ToBytes() {
        return HBaseSchemaConversions::utf8ToBytes;
    }

    /**
     * Decodes the UTF-8 bytes into a string
     */
    public static String utf8FromBytes(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    /**
     * Function to decode the string from UTF-8 bytes
     */
    public static Function<byte[], String> utf8FromBytes() {
        return HBaseSchemaConversions::utf8FromBytes;
    }

    /**
     * Applies a mapper function to the values in the source map
     *
     * @param source      source map
     * @param destination destination map
     * @param valueMapper function to map the source map value
     * @param <K>         key type
     * @param <V1>        source value type
     * @param <V2>        destination value type
     * @return the destination map
     */
    public static <T extends Map<K, V2>, K, V1, V2> T transformMapValues(Map<K, V1> source, T destination, Function<V1, V2> valueMapper) {
        for (Map.Entry<K, V1> entry : source.entrySet()) {
            K k = entry.getKey();
            V1 v1 = entry.getValue();
            V2 v2 = valueMapper.apply(v1);
            destination.put(k, v2);
        }
        return destination;
    }

    /**
     * Applies a mapper function to the keys and values in the source map
     *
     * @param source      source map
     * @param destination destination map
     * @param keyMapper   function to map the source map key
     * @param valueMapper function to map the source map value
     * @param <T>         map type
     * @param <K1>        source key type
     * @param <V1>        source value type
     * @param <K2>        destination key type
     * @param <V2>        destination value type
     * @return the destination map
     */
    public static <T extends Map<K2, V2>, K1, V1, K2, V2> T transformMap(Map<K1, V1> source,
                                                                         T destination,
                                                                         Function<K1, K2> keyMapper,
                                                                         Function<V1, V2> valueMapper) {
        for (Map.Entry<K1, V1> entry : source.entrySet()) {
            K1 k1 = entry.getKey();
            V1 v1 = entry.getValue();
            destination.put(keyMapper.apply(k1), valueMapper.apply(v1));
        }

        return destination;
    }
}
