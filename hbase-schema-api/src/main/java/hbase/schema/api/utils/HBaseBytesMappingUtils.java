package hbase.schema.api.utils;

import hbase.schema.api.interfaces.converters.HBaseBytesMapper;
import hbase.schema.api.interfaces.converters.HBaseLongMapper;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.function.Function;

/**
 * Utility class to build value extractors from standard types
 */
public final class HBaseBytesMappingUtils {
    private static final Base64.Decoder BASE64_DECODER = Base64.getDecoder();

    private HBaseBytesMappingUtils() {
        // utility class
    }

    static <T, F> HBaseBytesMapper<T> bytesMapper(Function<T, F> mapper, Function<F, byte[]> converter) {
        return obj -> {
            F value = mapper.apply(obj);
            return value != null ? converter.apply(value) : null;
        };
    }

    static <T, F> HBaseLongMapper<T> longMapper(Function<T, F> mapper, Function<F, Long> converter) {
        return obj -> {
            F value = mapper.apply(obj);
            return value != null ? converter.apply(value) : null;
        };
    }

    public static <T> HBaseBytesMapper<T> stringMapper(Function<T, String> getter) {
        return bytesMapper(getter, s -> s.getBytes(StandardCharsets.UTF_8));
    }

    public static <T> HBaseBytesMapper<T> toStringMapper(Function<T, ?> getter) {
        return stringMapper(getter, Object::toString);
    }

    public static <T, F> HBaseBytesMapper<T> stringMapper(Function<T, F> getter, Function<F, String> converter) {
        return bytesMapper(getter, value -> {
            String s = converter.apply(value);
            return s.getBytes(StandardCharsets.UTF_8);
        });
    }

    public static <T> HBaseBytesMapper<T> stringBase64BytesMapper(Function<T, String> getter) {
        return bytesMapper(getter, BASE64_DECODER::decode);
    }

    public static <T> HBaseBytesMapper<T> stringHexBytesMapper(Function<T, String> getter) {
        return bytesMapper(getter, s -> {
            try {
                return Hex.decodeHex(s);
            } catch (DecoderException e) {
                throw new IllegalArgumentException("Invalid input hex");
            }
        });
    }

    public static <T> HBaseBytesMapper<T> stringBinaryBytesMapper(Function<T, String> getter) {
        return bytesMapper(getter, Bytes::toBytesBinary);
    }

    public static <T> HBaseLongMapper<T> instantLongMapper(Function<T, Instant> getter) {
        return longMapper(getter, Instant::toEpochMilli);
    }

    public static <T> HBaseBytesMapper<T> instantStringMapper(Function<T, Instant> getter) {
        return stringMapper(getter, Instant::toString);
    }
}
