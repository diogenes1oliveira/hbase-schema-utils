package hbase.schema.api.utils;

import hbase.schema.api.interfaces.converters.HBaseBytesConverter;
import hbase.schema.api.interfaces.converters.HBaseLongConverter;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.function.Function;

public final class HBaseConverters {
    private HBaseConverters() {
        // utility class
    }

    private static final Base64.Decoder BASE64_DECODER = Base64.getMimeDecoder();
    private static final Base64.Encoder BASE64_ENCODER = Base64.getMimeEncoder();

    public static <T> HBaseBytesConverter<T> bytesConverter(Function<T, byte[]> toBytes, Function<byte[], T> fromBytes) {
        return new HBaseBytesConverter<T>() {
            @Override
            public Function<T, byte[]> toBytes() {
                return toBytes;
            }

            @Override
            public Function<byte[], T> fromBytes() {
                return fromBytes;
            }
        };
    }

    public static <T> HBaseLongConverter<T> longConverter(Function<T, Long> toLong, Function<Long, T> fromLong) {
        return new HBaseLongConverter<T>() {
            @Override
            public Function<T, Long> toLong() {
                return toLong;
            }

            @Override
            public Function<Long, T> fromLong() {
                return fromLong;
            }
        };
    }

    public static HBaseBytesConverter<String> utf8Converter() {
        return bytesConverter(
                s -> s.getBytes(StandardCharsets.UTF_8), bytes -> new String(bytes, StandardCharsets.UTF_8)
        );
    }

    public static <T> HBaseBytesConverter<String> utf8Converter(Function<T, String> toString, Function<String, T> fromString) {
        return bytesConverter(
                s -> s.getBytes(StandardCharsets.UTF_8), bytes -> new String(bytes, StandardCharsets.UTF_8)
        );
    }

    public static HBaseBytesConverter<String> base64Converter() {
        return bytesConverter(
                BASE64_DECODER::decode, BASE64_ENCODER::encodeToString
        );
    }

    public static HBaseBytesConverter<String> hexConverter() {
        return bytesConverter(
                s -> {
                    try {
                        return Hex.decodeHex(s);
                    } catch (DecoderException e) {
                        throw new IllegalArgumentException("Invalid hex input");
                    }
                }, Hex::encodeHexString
        );
    }

    public static HBaseBytesConverter<String> binaryStringConverter() {
        return bytesConverter(
                Bytes::toBytesBinary, Bytes::toStringBinary
        );
    }

    public static HBaseLongConverter<Instant> instantLongConverter() {
        return longConverter(Instant::toEpochMilli, Instant::ofEpochMilli);
    }

    public static HBaseBytesConverter<String> instantStringConverter() {
        return utf8Converter(Instant::toString, Instant::parse);
    }

}
