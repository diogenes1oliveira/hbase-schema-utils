package hbase.schema.api.interfaces.converters;

import java.util.function.Function;

public interface HBaseBytesConverter<T> {
    Function<T, byte[]> toBytes();

    Function<byte[], T> fromBytes();

}
