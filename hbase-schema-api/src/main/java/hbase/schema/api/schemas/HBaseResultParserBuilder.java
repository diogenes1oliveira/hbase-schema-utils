package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseResultParser;

import java.nio.charset.StandardCharsets;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static hbase.schema.api.utils.HBaseFunctionals.dummyBiConsumer;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;

public class HBaseResultParserBuilder<T> {
    private final Supplier<T> constructor;
    private BiConsumer<T, byte[]> fromRowKeySetter = dummyBiConsumer();
    private final TreeMap<byte[], BiConsumer<T, byte[]>> fromCellSetters = asBytesTreeMap();
    private final TreeMap<byte[], BiConsumer<T, NavigableMap<byte[], byte[]>>> fromPrefixCellSetters = asBytesTreeMap();

    public HBaseResultParserBuilder(Supplier<T> constructor) {
        this.constructor = constructor;
    }

    public HBaseResultParserBuilder<T> fromRowKey(BiConsumer<T, byte[]> bytesSetter) {
        this.fromRowKeySetter = bytesSetter;
        return this;
    }

    public <F> HBaseResultParserBuilder<T> fromRowKey(BiConsumer<T, F> setter, Function<byte[], F> converter) {
        return fromRowKey((obj, bytes) -> {
            F field = converter.apply(bytes);
            setter.accept(obj, field);
        });
    }

    public HBaseResultParserBuilder<T> fromColumn(byte[] qualifier, BiConsumer<T, byte[]> bytesSetter) {
        fromCellSetters.put(qualifier, bytesSetter);
        return this;
    }

    public <F> HBaseResultParserBuilder<T> fromColumn(byte[] qualifier, BiConsumer<T, F> setter, Function<byte[], F> converter) {
        return fromColumn(qualifier, (obj, bytes) -> {
            F field = converter.apply(bytes);
            setter.accept(obj, field);
        });
    }

    public HBaseResultParserBuilder<T> fromColumn(String qualifier, BiConsumer<T, byte[]> bytesSetter) {
        return fromColumn(qualifier.getBytes(StandardCharsets.UTF_8), bytesSetter);
    }

    public <F> HBaseResultParserBuilder<T> fromColumn(String qualifier, BiConsumer<T, F> setter, Function<byte[], F> converter) {
        return fromColumn(qualifier.getBytes(StandardCharsets.UTF_8), setter, converter);
    }

    public HBaseResultParserBuilder<T> fromPrefix(byte[] prefix, BiConsumer<T, NavigableMap<byte[], byte[]>> cellsSetter) {
        fromPrefixCellSetters.put(prefix, cellsSetter);
        return this;
    }

    public <F> HBaseResultParserBuilder<T> fromPrefix(byte[] prefix, BiConsumer<T, F> setter, Function<NavigableMap<byte[], byte[]>, F> converter) {
        return fromPrefix(prefix, (obj, cells) -> {
            F field = converter.apply(cells);
            setter.accept(obj, field);
        });
    }

    public HBaseResultParserBuilder<T> fromPrefix(String prefix, BiConsumer<T, NavigableMap<byte[], byte[]>> cellsSetter) {
        return fromPrefix(prefix.getBytes(StandardCharsets.UTF_8), cellsSetter);
    }

    public <F> HBaseResultParserBuilder<T> fromPrefix(String prefix,
                                                      BiConsumer<T, F> setter,
                                                      Function<NavigableMap<byte[], byte[]>, F> converter) {
        return fromPrefix(prefix.getBytes(StandardCharsets.UTF_8), setter, converter);
    }

    public HBaseResultParser<T> build() {
        return new HBaseFunctionalResultParser<>(
                constructor,
                fromRowKeySetter,
                fromCellSetters,
                fromPrefixCellSetters
        );
    }

}
