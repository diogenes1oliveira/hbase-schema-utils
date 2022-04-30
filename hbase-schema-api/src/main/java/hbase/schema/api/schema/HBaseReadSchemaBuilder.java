package hbase.schema.api.schema;

import hbase.base.interfaces.TriConsumer;
import hbase.schema.api.interfaces.HBaseByteMapper;
import hbase.schema.api.interfaces.HBaseByteParser;
import hbase.schema.api.interfaces.HBaseBytesMapper;
import hbase.schema.api.interfaces.HBaseCellParser;
import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.api.interfaces.conversion.BytesConverter;
import hbase.schema.api.utils.ByteBufferComparator;
import hbase.schema.api.utils.ByteBufferPrefixComparator;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static hbase.schema.api.interfaces.HBaseByteParser.hBaseByteParser;
import static hbase.schema.api.interfaces.HBaseCellParser.hBaseCellParser;
import static hbase.schema.api.utils.HBaseSchemaUtils.chain;
import static hbase.schema.api.utils.HBaseSchemaUtils.chainMap;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class HBaseReadSchemaBuilder<Q, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseReadSchemaBuilder.class);

    private final Supplier<R> resultCreator;
    private final List<HBaseByteParser<R>> rowKeyParsers = new ArrayList<>();
    private final List<HBaseCellParser<R>> cellParsers = asList(this::parseFixedCell, this::parsePrefixCell);
    private HBaseByteMapper<Q> rowKeyMapper = null;
    private HBaseBytesMapper<Q> scanStartsMapper = HBaseBytesMapper.empty();
    private HBaseBytesMapper<Q> scanStopsMapper = HBaseBytesMapper.empty();
    private final SortedMap<ByteBuffer, HBaseByteParser<R>> fixedCellParsers = new TreeMap<>(ByteBufferComparator.INSTANCE);
    private final SortedMap<ByteBuffer, HBaseCellParser<R>> prefixCellParsers = new TreeMap<>(ByteBufferPrefixComparator.INSTANCE);

    /**
     * @param resultCreator lambda to create fresh result instances
     */
    public HBaseReadSchemaBuilder(Supplier<R> resultCreator) {
        this.resultCreator = resultCreator;
    }


    /**
     * Obs: this constructor is used mostly to aid in the fluent interface
     *
     * @param resultCreator lambda to create fresh result instances
     */
    public HBaseReadSchemaBuilder(Supplier<R> resultCreator, Class<Q> queryType) {
        this(resultCreator);
    }

    public HBaseReadSchemaBuilder<Q, R> parseAny(HBaseCellParser<R> cellParser) {
        cellParsers.add(cellParser);
        return this;
    }

    public HBaseReadSchemaBuilder<Q, R> parsePrefix(byte[] prefix, HBaseCellParser<R> cellParser) {
        this.prefixCellParsers.put(ByteBuffer.wrap(prefix), cellParser);
        return this;
    }

    public HBaseReadSchemaBuilder<Q, R> parsePrefix(String prefix, HBaseCellParser<R> cellParser) {
        return parsePrefix(prefix.getBytes(StandardCharsets.UTF_8), cellParser);
    }

    public <K, V> HBaseReadSchemaBuilder<Q, R> parsePrefix(byte[] prefix,
                                                           TriConsumer<R, K, V> setter,
                                                           Function<ByteBuffer, K> keyConverter,
                                                           Function<ByteBuffer, V> valueConverter) {
        return parsePrefix(prefix, hBaseCellParser(setter, keyConverter, valueConverter));
    }

    public <K, V> HBaseReadSchemaBuilder<Q, R> parsePrefix(byte[] prefix,
                                                           TriConsumer<R, K, V> setter,
                                                           BytesConverter<K> keyConverter,
                                                           BytesConverter<V> valueConverter) {
        return parsePrefix(prefix, setter, keyConverter::fromBytes, valueConverter::fromBytes);
    }

    public <K, V> HBaseReadSchemaBuilder<Q, R> parsePrefix(String prefix,
                                                           TriConsumer<R, K, V> setter,
                                                           Function<ByteBuffer, K> keyConverter,
                                                           Function<ByteBuffer, V> valueConverter) {
        return parsePrefix(prefix.getBytes(StandardCharsets.UTF_8), setter, keyConverter, valueConverter);
    }

    public <K, V> HBaseReadSchemaBuilder<Q, R> parsePrefix(String prefix,
                                                           TriConsumer<R, K, V> setter,
                                                           BytesConverter<K> keyConverter,
                                                           BytesConverter<V> valueConverter) {
        return parsePrefix(prefix, setter, keyConverter::fromBytes, valueConverter::fromBytes);
    }

    public HBaseReadSchemaBuilder<Q, R> parseColumn(byte[] qualifier, HBaseByteParser<R> byteParser) {
        this.fixedCellParsers.put(ByteBuffer.wrap(qualifier), byteParser);
        return this;
    }

    public HBaseReadSchemaBuilder<Q, R> parseColumn(String qualifier, HBaseByteParser<R> byteParser) {
        return parseColumn(qualifier.getBytes(StandardCharsets.UTF_8), byteParser);
    }

    public <T> HBaseReadSchemaBuilder<Q, R> parseColumn(byte[] qualifier, BiConsumer<R, T> setter, Function<ByteBuffer, T> converter) {
        return parseColumn(qualifier, hBaseByteParser(setter, converter));
    }

    public <T> HBaseReadSchemaBuilder<Q, R> parseColumn(String qualifier, BiConsumer<R, T> setter, Function<ByteBuffer, T> converter) {
        return parseColumn(qualifier.getBytes(StandardCharsets.UTF_8), setter, converter);
    }

    public <T> HBaseReadSchemaBuilder<Q, R> parseColumn(byte[] qualifier, BiConsumer<R, T> setter, BytesConverter<T> converter) {
        return parseColumn(qualifier, setter, converter::fromBytes);
    }

    public <T> HBaseReadSchemaBuilder<Q, R> parseColumn(String qualifier, BiConsumer<R, T> setter, BytesConverter<T> converter) {
        return parseColumn(qualifier.getBytes(StandardCharsets.UTF_8), setter, converter);
    }

    public HBaseReadSchemaBuilder<Q, R> parseRowKey(HBaseByteParser<R> parser) {
        rowKeyParsers.add(parser);
        return this;
    }

    public <T> HBaseReadSchemaBuilder<Q, R> parseRowKey(BiConsumer<R, T> setter, Function<ByteBuffer, T> converter) {
        return parseRowKey(hBaseByteParser(setter, converter));
    }

    public <T> HBaseReadSchemaBuilder<Q, R> parseRowKey(BiConsumer<R, T> setter, BytesConverter<T> converter) {
        return parseRowKey(setter, converter::fromBytes);
    }

    public HBaseReadSchemaBuilder<Q, R> getKey(HBaseByteMapper<Q> mapper) {
        this.rowKeyMapper = mapper;
        return this;
    }

    public <T> HBaseReadSchemaBuilder<Q, R> getKey(Function<Q, T> getter, Function<T, ByteBuffer> converter) {
        return getKey(chain(getter, converter)::apply);
    }

    public <T> HBaseReadSchemaBuilder<Q, R> getKey(Function<Q, T> getter, BytesConverter<T> converter) {
        return getKey(getter, converter::toBuffer);
    }

    public HBaseReadSchemaBuilder<Q, R> scanKey(HBaseByteMapper<Q> mapper) {
        return scanStart(mapper).scanStops(HBaseBytesMapper.singleton(null));
    }

    public <T> HBaseReadSchemaBuilder<Q, R> scanKey(Function<Q, T> getter, Function<T, ByteBuffer> converter) {
        return scanKey(chain(getter, converter)::apply);
    }

    public <T> HBaseReadSchemaBuilder<Q, R> scanKey(Function<Q, T> getter, BytesConverter<T> converter) {
        return scanKey(getter, converter::toBuffer);
    }

    public HBaseReadSchemaBuilder<Q, R> scanKey(HBaseByteMapper<Q> mapper, int size) {
        return scanKey(mapper.crop(size));
    }

    public <T> HBaseReadSchemaBuilder<Q, R> scanKey(Function<Q, T> getter, Function<T, ByteBuffer> converter, int size) {
        return scanKey(chain(getter, converter)::apply, size);
    }

    public HBaseReadSchemaBuilder<Q, R> scanKeys(HBaseBytesMapper<Q> mapper) {
        return scanStarts(mapper).scanStops(mapper.andThen(b -> null));
    }

    public <T> HBaseReadSchemaBuilder<Q, R> scanKeys(Function<Q, List<T>> getter, Function<T, ByteBuffer> converter) {
        return scanKeys(chainMap(getter, converter)::apply);
    }

    public <T> HBaseReadSchemaBuilder<Q, R> scanKeys(Function<Q, List<T>> getter, BytesConverter<T> converter) {
        return scanKeys(getter, converter::toBuffer);
    }

    public <T> HBaseReadSchemaBuilder<Q, R> scanKey(Function<Q, T> getter, BytesConverter<T> converter, int size) {
        return scanKey(getter, converter::toBuffer, size);
    }

    public HBaseReadSchemaBuilder<Q, R> scanStarts(HBaseBytesMapper<Q> mapper) {
        this.scanStartsMapper = mapper;
        return this;
    }

    public <T> HBaseReadSchemaBuilder<Q, R> scanStarts(Function<Q, List<T>> getter, Function<T, ByteBuffer> converter) {
        return scanStarts(chainMap(getter, converter)::apply);
    }

    public <T> HBaseReadSchemaBuilder<Q, R> scanStarts(Function<Q, List<T>> getter, BytesConverter<T> converter) {
        return scanStarts(getter, converter::toBuffer);
    }

    public HBaseReadSchemaBuilder<Q, R> scanStart(HBaseByteMapper<Q> mapper) {
        return scanStarts(q -> singletonList(mapper.toBuffer(q)));
    }

    public <T> HBaseReadSchemaBuilder<Q, R> scanStart(Function<Q, T> getter, Function<T, ByteBuffer> converter) {
        return scanStart(chain(getter, converter)::apply);
    }

    public <T> HBaseReadSchemaBuilder<Q, R> scanStart(Function<Q, T> getter, BytesConverter<T> converter) {
        return scanStart(getter, converter::toBuffer);
    }

    public HBaseReadSchemaBuilder<Q, R> scanStops(HBaseBytesMapper<Q> mapper) {
        this.scanStopsMapper = mapper;
        return this;
    }

    public <T> HBaseReadSchemaBuilder<Q, R> scanStops(Function<Q, List<T>> getter, Function<T, ByteBuffer> converter) {
        return scanStops(chainMap(getter, converter)::apply);
    }

    public <T> HBaseReadSchemaBuilder<Q, R> scanStops(Function<Q, List<T>> getter, BytesConverter<T> converter) {
        return scanStops(getter, converter::toBuffer);
    }

    public HBaseReadSchemaBuilder<Q, R> scanStop(HBaseByteMapper<Q> mapper) {
        return scanStops(q -> singletonList(mapper.toBuffer(q)));
    }

    public <T> HBaseReadSchemaBuilder<Q, R> scanStop(Function<Q, T> getter, Function<T, ByteBuffer> converter) {
        return scanStop(chain(getter, converter)::apply);
    }

    public <T> HBaseReadSchemaBuilder<Q, R> scanStop(Function<Q, T> getter, BytesConverter<T> converter) {
        return scanStop(getter, converter::toBuffer);
    }

    public HBaseReadSchema<Q, R> build() {

        return new HBaseReadSchema<Q, R>() {
            @Override
            public List<Scan> toScans(Q query) {
                List<Scan> scans = new ArrayList<>();

                for (Pair<byte[], byte[]> pair : generateScanLimits(query)) {
                    byte[] scanStart = pair.getLeft();
                    byte[] scanStop = pair.getRight();

                    LOGGER.info("Built scan keys: {} -> {}", Bytes.toStringBinary(scanStart), scanStop != null ?
                            Bytes.toStringBinary(scanStop) : "null");
                    if (scanStop != null) {
                        scans.add(new Scan().withStartRow(scanStart).withStopRow(scanStop));
                    } else {
                        scans.add(new Scan().setRowPrefixFilter(scanStart));
                    }
                }
                return scans;
            }

            @Override
            public Get toGet(Q query) {
                if (rowKeyMapper == null) {
                    return HBaseReadSchema.super.toGet(query);
                } else {
                    ByteBuffer rowKey = rowKeyMapper.toBuffer(query);
                    LOGGER.info("Built row key: {}", Bytes.toStringBinary(rowKey));
                    return new Get(rowKey);
                }
            }

            @Override
            public R newInstance() {
                return resultCreator.get();
            }

            @Override
            public boolean parseRowKey(R result, ByteBuffer rowKey, Q query) {
                boolean parsed = false;

                for (HBaseByteParser<R> parser : rowKeyParsers) {
                    parsed = parser.parse(result, rowKey) || parsed;
                }

                return true;
            }

            @Override
            public boolean parseCell(R result, ByteBuffer qualifier, ByteBuffer value, Q query) {
                boolean parsed = false;

                for (HBaseCellParser<R> parser : cellParsers) {
                    parsed = parser.parse(result, qualifier, value) || parsed;
                }

                return parsed;
            }
        };
    }

    private List<Pair<byte[], byte @Nullable []>> generateScanLimits(Q query) {
        List<ByteBuffer> scanStarts = scanStartsMapper.toBuffers(query);
        List<ByteBuffer> scanStops = scanStopsMapper.toBuffers(query);
        if (scanStarts.size() != scanStops.size()) {
            throw new IllegalStateException("Got " + scanStarts.size() + " scan starts but " + scanStops.size() + "scan " +
                    "stops");
        }
        List<Pair<byte[], byte[]>> pairs = new ArrayList<>();

        for (int i = 0; i < scanStarts.size(); ++i) {
            ByteBuffer scanStart = scanStarts.get(i);
            ByteBuffer scanStop = scanStops.get(i);

            if (scanStop != null) {
                pairs.add(Pair.of(toBytes(scanStart), toBytes(scanStop)));
            } else {
                pairs.add(Pair.of(toBytes(scanStart), null));
            }
        }

        return pairs;
    }

    private boolean parseFixedCell(R result, ByteBuffer column, ByteBuffer value) {
        HBaseByteParser<R> parser = fixedCellParsers.get(column);

        if (parser != null) {
            return parser.parse(result, value);
        } else {
            return false;
        }
    }

    private boolean parsePrefixCell(R result, ByteBuffer column, ByteBuffer value) {
        HBaseCellParser<R> parser = prefixCellParsers.get(column);

        if (parser != null) {
            ByteBuffer prefix = prefixCellParsers.tailMap(column).firstKey();
            return parser.parse(result, (ByteBuffer) column.position(prefix.limit()), value);
        } else {
            return false;
        }
    }

}
