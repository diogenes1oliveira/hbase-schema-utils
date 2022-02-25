package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseFromBytesMapSetter;
import hbase.schema.api.interfaces.HBaseFromBytesSetter;
import hbase.schema.api.interfaces.HBaseResultParser;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.function.Supplier;

import static hbase.schema.api.utils.HBaseFunctionals.mapBytesKeys;

public class HBaseResultParserBuilder<T> {
    private final Supplier<T> constructor;
    private HBaseFromBytesSetter<T> fromRowKeySetter = HBaseFromBytesSetter.dummy();
    private final List<HBaseFromBytesMapSetter<T>> fromBytesMapSetters = new ArrayList<>();

    public HBaseResultParserBuilder(Supplier<T> constructor) {
        this.constructor = constructor;
    }

    public HBaseResultParserBuilder<T> fromRowKey(HBaseFromBytesSetter<T> bytesSetter) {
        this.fromRowKeySetter = bytesSetter;
        return this;
    }

    public HBaseResultParserBuilder<T> fromColumn(byte[] qualifier, HBaseFromBytesSetter<T> bytesSetter) {
        fromBytesMapSetters.add((obj, bytesMap) -> {
            byte[] value = bytesMap.get(qualifier);
            if (value != null) {
                bytesSetter.setFromBytes(obj, value);
            }
        });
        return this;
    }

    public HBaseResultParserBuilder<T> fromColumn(String qualifier, HBaseFromBytesSetter<T> bytesSetter) {
        return fromColumn(qualifier.getBytes(StandardCharsets.UTF_8), bytesSetter);
    }

    public HBaseResultParserBuilder<T> fromPrefix(byte[] prefix, HBaseFromBytesMapSetter<T> bytesMapSetter) {
        byte[] prefixBefore = Bytes.incrementBytes(prefix, -1L);
        byte[] prefixAfter = Bytes.incrementBytes(prefix, +1L);

        fromBytesMapSetters.add((obj, bytesMap) -> {
            NavigableMap<byte[], byte[]> prefixBytesMap = bytesMap.subMap(
                    prefixBefore, false, prefixAfter, false
            );
            if (!prefixBytesMap.isEmpty()) {
                NavigableMap<byte[], byte[]> unprefixedBytesMap = mapBytesKeys(prefixBytesMap, qualifier -> unprefix(qualifier, prefix));
                bytesMapSetter.setFromBytes(obj, unprefixedBytesMap);
            }
        });
        return this;
    }

    public <F> HBaseResultParserBuilder<T> fromPrefix(String prefix, HBaseFromBytesMapSetter<T> bytesMapSetter) {
        return fromPrefix(prefix.getBytes(StandardCharsets.UTF_8), bytesMapSetter);
    }

    public HBaseResultParser<T> build() {
        return new HBaseResultParser<T>() {
            @Override
            public T newInstance() {
                return constructor.get();
            }

            @Override
            public void setFromRowKey(T obj, byte[] rowKey) {
                fromRowKeySetter.setFromBytes(obj, rowKey);
            }

            @Override
            public void setFromResult(T obj, NavigableMap<byte[], byte[]> cellsMap) {
                for (HBaseFromBytesMapSetter<T> setter : fromBytesMapSetters) {
                    setter.setFromBytes(obj, cellsMap);
                }
            }
        };
    }

    private static byte[] unprefix(byte[] value, byte[] prefix) {
        return Arrays.copyOfRange(value, prefix.length, value.length);
    }

}
