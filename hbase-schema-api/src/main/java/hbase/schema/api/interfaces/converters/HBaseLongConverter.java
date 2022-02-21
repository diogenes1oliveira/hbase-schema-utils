package hbase.schema.api.interfaces.converters;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.function.Function;

public interface HBaseLongConverter<T> extends HBaseBytesConverter<T> {
    Function<T, Long> toLong();

    Function<Long, T> fromLong();

    @Override
    default Function<T, byte[]> toBytes() {
        return obj -> {
            Long l = toLong().apply(obj);
            if (l == null) {
                return null;
            } else {
                return Bytes.toBytes(l);
            }
        };
    }

    @Override
    default Function<byte[], T> fromBytes() {
        return bytes -> {
            Long l = Bytes.toLong(bytes);
            return fromLong().apply(l);
        };
    }

}
