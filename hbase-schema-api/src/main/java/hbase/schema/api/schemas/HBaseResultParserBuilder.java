package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseResultParser;
import hbase.schema.api.interfaces.conversion.BytesConverter;
import hbase.schema.api.interfaces.conversion.BytesMapConverter;
import hbase.schema.api.models.HBaseValueCell;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static hbase.schema.api.utils.HBaseSchemaUtils.chain;

public class HBaseResultParserBuilder<T> {
    private final Supplier<T> constructor;
    private BiConsumer<T, byte[]> rowKeyParser = null;
    private final Map<byte[], BiConsumer<T, byte[]>> columnParsers = asBytesTreeMap();
    private final Map<byte[], BiConsumer<T, NavigableMap<byte[], byte[]>>> prefixParsers = asBytesTreeMap();

    public HBaseResultParserBuilder(Supplier<T> constructor) {
        this.constructor = constructor;
    }

    public HBaseResultParserBuilder<T> rowKeyBytes(BiConsumer<T, byte[]> rowKeyParser) {
        this.rowKeyParser = rowKeyParser;
        return this;
    }

    public <F> HBaseResultParserBuilder<T> rowKey(BiConsumer<T, F> setter, Function<byte[], F> converter) {
        return rowKeyBytes(chain(setter, converter));
    }

    public <F> HBaseResultParserBuilder<T> rowKey(BiConsumer<T, F> setter, BytesConverter<F> converter) {
        return rowKey(setter, converter::fromBytes);
    }

    public HBaseResultParserBuilder<T> columnBytes(byte[] qualifier, BiConsumer<T, byte[]> cellParser) {
        columnParsers.put(qualifier, cellParser);
        return this;
    }

    public <F> HBaseResultParserBuilder<T> column(byte[] qualifier, BiConsumer<T, F> setter, Function<byte[], F> converter) {
        return columnBytes(qualifier, chain(setter, converter));
    }

    public <F> HBaseResultParserBuilder<T> column(byte[] qualifier, BiConsumer<T, F> setter, BytesConverter<F> converter) {
        return column(qualifier, setter, converter::fromBytes);
    }

    public HBaseResultParserBuilder<T> columnBytes(String qualifier, BiConsumer<T, byte[]> cellParser) {
        return columnBytes(qualifier.getBytes(StandardCharsets.UTF_8), cellParser);
    }

    public <F> HBaseResultParserBuilder<T> column(String qualifier, BiConsumer<T, F> setter, Function<byte[], F> converter) {
        return column(qualifier.getBytes(StandardCharsets.UTF_8), setter, converter);
    }

    public <F> HBaseResultParserBuilder<T> column(String qualifier, BiConsumer<T, F> setter, BytesConverter<F> converter) {
        return column(qualifier.getBytes(StandardCharsets.UTF_8), setter, converter);
    }

    public HBaseResultParserBuilder<T> prefixBytes(byte[] prefix, BiConsumer<T, NavigableMap<byte[], byte[]>> cellsParser) {
        prefixParsers.put(prefix, cellsParser);
        return this;
    }

    public <F> HBaseResultParserBuilder<T> prefix(byte[] prefix,
                                                  BiConsumer<T, F> setter,
                                                  Function<NavigableMap<byte[], byte[]>, F> converter) {
        return prefixBytes(prefix, chain(setter, converter));
    }

    public <F> HBaseResultParserBuilder<T> prefix(byte[] prefix, BiConsumer<T, F> setter, BytesMapConverter<F> converter) {
        return prefix(prefix, setter, converter::fromBytesMap);
    }

    public HBaseResultParserBuilder<T> prefixBytes(String prefix, BiConsumer<T, NavigableMap<byte[], byte[]>> cellsParser) {
        return prefixBytes(prefix.getBytes(StandardCharsets.UTF_8), cellsParser);
    }

    public <F> HBaseResultParserBuilder<T> prefix(String prefix,
                                                  BiConsumer<T, F> setter,
                                                  Function<NavigableMap<byte[], byte[]>, F> converter) {
        return prefix(prefix.getBytes(StandardCharsets.UTF_8), setter, converter);
    }

    public <F> HBaseResultParserBuilder<T> prefix(String prefix, BiConsumer<T, F> setter, BytesMapConverter<F> converter) {
        return prefix(prefix.getBytes(StandardCharsets.UTF_8), setter, converter);
    }

    public HBaseResultParser<T> build() {
        return new AbstractHBaseResultParser<T>() {
            @Override
            public Set<byte[]> prefixes() {
                return prefixParsers.keySet();
            }

            @Override
            public void parseCell(T obj, HBaseValueCell cell) {
                BiConsumer<T, byte[]> parser = columnParsers.get(cell.getQualifier());
                if (parser != null) {
                    parser.accept(obj, cell.getValue());
                }
            }

            @Override
            public void parseCells(T obj, byte[] prefix, NavigableMap<byte[], byte[]> prefixMap) {
                BiConsumer<T, NavigableMap<byte[], byte[]>> cellsParser = prefixParsers.get(prefix);
                if (cellsParser != null) {
                    cellsParser.accept(obj, prefixMap);
                }
            }

            @Override
            public void parseRowKey(T obj, byte[] rowKey) {
                if (rowKeyParser != null) {
                    rowKeyParser.accept(obj, rowKey);
                }
            }

            @Override
            public T newInstance() {
                return constructor.get();
            }
        };
    }
}
