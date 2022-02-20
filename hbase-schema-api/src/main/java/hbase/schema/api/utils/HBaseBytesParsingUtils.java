package hbase.schema.api.utils;

import hbase.schema.api.interfaces.converters.HBaseBytesParser;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.function.BiConsumer;
import java.util.function.Function;


/**
 * Utility class to parse byte[] values into standard types
 */
public final class HBaseBytesParsingUtils {

    private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

    private HBaseBytesParsingUtils() {
        // utility class
    }

    static <T, F> HBaseBytesParser<T> bytesParser(BiConsumer<T, F> setter, Function<byte[], F> converter) {
        return (obj, bytes) -> {
            F value = converter.apply(bytes);
            setter.accept(obj, value);
        };
    }

    public static <T> HBaseBytesParser<T> longParser(BiConsumer<T, Long> setter) {
        return bytesParser(setter, Bytes::toLong);
    }

    public static <T> HBaseBytesParser<T> stringParser(BiConsumer<T, String> setter) {
        return bytesParser(setter, bytes -> new String(bytes, StandardCharsets.UTF_8));
    }

    public static <T, F> HBaseBytesParser<T> stringParser(BiConsumer<T, F> setter, Function<String, F> converter) {
        return stringParser((obj, s) -> {
            F field = converter.apply(s);
            setter.accept(obj, field);
        });
    }

    public static <T> HBaseBytesParser<T> stringBase64BytesParser(BiConsumer<T, String> setter) {
        return bytesParser(setter, BASE64_ENCODER::encodeToString);
    }

    public static <T> HBaseBytesParser<T> stringHexBytesParser(BiConsumer<T, String> setter) {
        return bytesParser(setter, Hex::encodeHexString);
    }

    public static <T> HBaseBytesParser<T> stringBinaryBytesParser(BiConsumer<T, String> setter) {
        return bytesParser(setter, Bytes::toStringBinary);
    }

    public static <T> HBaseBytesParser<T> instantLongParser(BiConsumer<T, Instant> setter) {
        return longParser((obj, l) -> {
            Instant instant = Instant.ofEpochMilli(l);
            setter.accept(obj, instant);
        });
    }

    public static <T> HBaseBytesParser<T> instantStringParser(BiConsumer<T, Instant> setter) {
        return stringParser((obj, s) -> {
            Instant instant = Instant.parse(s);
            setter.accept(obj, instant);
        });
    }
}
