package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseResultParserSchema;
import hbase.schema.api.interfaces.conversion.BytesConverter;
import hbase.schema.api.interfaces.conversion.BytesMapConverter;

import java.nio.charset.StandardCharsets;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static hbase.schema.api.utils.HBaseSchemaUtils.chain;

/**
 * Builder for {@link HBaseResultParserSchema} objects, providing a fluent API to add fields and field prefixes to be parsed
 *
 * @param <T> result object instance
 */
public class HBaseResultParserSchemaBuilder<T> {
    private final Supplier<T> constructor;
    private BiConsumer<T, byte[]> fromRowKeySetter = (obj, bytes) -> {
        // dummy
    };
    private final NavigableMap<byte[], BiConsumer<T, byte[]>> cellSetters = asBytesTreeMap();
    private final NavigableMap<byte[], BiConsumer<T, NavigableMap<byte[], byte[]>>> prefixSetters = asBytesTreeMap();

    /**
     * Instantiate a new builder
     *
     * @param constructor supplier of a new result instance
     */
    public HBaseResultParserSchemaBuilder(Supplier<T> constructor) {
        this.constructor = constructor;
    }

    /**
     * Sets up the parsing of the row key
     *
     * @param bytesSetter lambda to populate the result object with data from the row key
     * @return this builder
     */
    public HBaseResultParserSchemaBuilder<T> fromRowKey(BiConsumer<T, byte[]> bytesSetter) {
        this.fromRowKeySetter = bytesSetter;
        return this;
    }

    /**
     * Sets up the parsing of the row key
     *
     * @param setter    lambda to populate the result object with data from a converted row key value
     * @param converter converts the row key bytes to a proper value
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseResultParserSchemaBuilder<T> fromRowKey(BiConsumer<T, U> setter, Function<byte[], U> converter) {
        return fromRowKey(chain(setter, converter));
    }

    /**
     * Sets up the parsing of the row key
     *
     * @param setter    lambda to populate the result object with data from a converted row key value
     * @param converter converts the row key bytes to a proper value
     * @param <U>       value type
     * @return this builder
     */
    public <U> HBaseResultParserSchemaBuilder<T> fromRowKey(BiConsumer<T, U> setter, BytesConverter<U> converter) {
        return fromRowKey(setter, converter::fromBytes);
    }

    /**
     * Sets up the parsing of a fixed column
     *
     * @param qualifier   fixed column qualifier bytes
     * @param bytesSetter lambda to populate the result object with data from the given column
     * @return this builder
     */
    public HBaseResultParserSchemaBuilder<T> fromColumn(byte[] qualifier, BiConsumer<T, byte[]> bytesSetter) {
        cellSetters.put(qualifier, bytesSetter);
        return this;
    }

    /**
     * Sets up the parsing of a fixed column
     *
     * @param qualifier fixed column qualifier bytes
     * @param setter    lambda to populate the result object with the converted value from the given column
     * @param converter converts the column bytes to a proper value
     * @param <U>       converted value type
     * @return this builder
     */
    public <U> HBaseResultParserSchemaBuilder<T> fromColumn(byte[] qualifier,
                                                            BiConsumer<T, U> setter,
                                                            Function<byte[], U> converter) {
        return fromColumn(qualifier, chain(setter, converter));
    }

    /**
     * Sets up the parsing of a fixed column
     *
     * @param qualifier fixed column qualifier bytes
     * @param setter    lambda to populate the result object with the converted value from the given column
     * @param converter converts the column bytes to a proper value
     * @param <U>       converted value type
     * @return this builder
     */
    public <U> HBaseResultParserSchemaBuilder<T> fromColumn(byte[] qualifier,
                                                            BiConsumer<T, U> setter,
                                                            BytesConverter<U> converter) {
        return fromColumn(qualifier, setter, converter::fromBytes);
    }

    /**
     * Sets up the parsing of a fixed column
     *
     * @param qualifier   fixed column qualifier UTF-8 string
     * @param bytesSetter lambda to populate the result object with data from the given column
     * @return this builder
     */
    public HBaseResultParserSchemaBuilder<T> fromColumn(String qualifier, BiConsumer<T, byte[]> bytesSetter) {
        return fromColumn(qualifier.getBytes(StandardCharsets.UTF_8), bytesSetter);
    }

    /**
     * Sets up the parsing of a fixed column
     *
     * @param qualifier fixed column qualifier UTF-8 string
     * @param setter    lambda to populate the result object with the converted value from the given column
     * @param converter converts the column bytes to a proper value
     * @param <U>       converted value type
     * @return this builder
     */
    public <U> HBaseResultParserSchemaBuilder<T> fromColumn(String qualifier,
                                                            BiConsumer<T, U> setter,
                                                            Function<byte[], U> converter) {
        return fromColumn(qualifier.getBytes(StandardCharsets.UTF_8), setter, converter);
    }

    /**
     * Sets up the parsing of a fixed column
     *
     * @param qualifier fixed column qualifier UTF-8 string
     * @param setter    lambda to populate the result object with the converted value from the given column
     * @param converter converts the column bytes to a proper value
     * @param <U>       converted value type
     * @return this builder
     */
    public <U> HBaseResultParserSchemaBuilder<T> fromColumn(String qualifier,
                                                            BiConsumer<T, U> setter,
                                                            BytesConverter<U> converter) {
        return fromColumn(qualifier.getBytes(StandardCharsets.UTF_8), setter, converter);
    }

    /**
     * Sets up the parsing of a set of columns with the given prefix
     *
     * @param prefix         cells qualifier prefix bytes
     * @param bytesMapSetter lambda to populate the result object with data from the given prefixes. This setter
     *                       will receive the data without such prefix
     * @return this builder
     */
    public HBaseResultParserSchemaBuilder<T> fromPrefix(byte[] prefix,
                                                        BiConsumer<T, NavigableMap<byte[], byte[]>> bytesMapSetter) {
        prefixSetters.put(prefix, bytesMapSetter);
        return this;
    }

    /**
     * Sets up the parsing of a set of columns with the given prefix
     *
     * @param prefix    cells qualifier prefix bytes
     * @param setter    lambda to populate the result object with converted data from the given prefixes
     * @param converter converts the column bytes to a proper value. This converter will receive the bytes map
     *                  without the qualifier prefix
     * @param <U>       converted value type
     * @return this builder
     */
    public <U> HBaseResultParserSchemaBuilder<T> fromPrefix(byte[] prefix,
                                                            BiConsumer<T, U> setter,
                                                            Function<NavigableMap<byte[], byte[]>, U> converter) {
        return fromPrefix(prefix, chain(setter, converter));
    }

    /**
     * Sets up the parsing of a set of columns with the given prefix
     *
     * @param prefix    cells qualifier prefix bytes
     * @param setter    lambda to populate the result object with converted data from the given prefixes
     * @param converter converts the column bytes to a proper value. This converter will receive the bytes map
     *                  without the qualifier prefix
     * @param <U>       converted value type
     * @return this builder
     */
    public <U> HBaseResultParserSchemaBuilder<T> fromPrefix(byte[] prefix,
                                                            BiConsumer<T, U> setter,
                                                            BytesMapConverter<U> converter) {
        return fromPrefix(prefix, setter, converter::fromBytesMap);
    }

    /**
     * Sets up the parsing of a set of columns with the given prefix
     *
     * @param prefix         cells qualifier prefix UTF-8 string
     * @param bytesMapSetter lambda to populate the result object with data from the given prefixes. This setter
     *                       will receive the data without such prefix
     * @return this builder
     */
    public HBaseResultParserSchemaBuilder<T> fromPrefix(String prefix,
                                                        BiConsumer<T, NavigableMap<byte[], byte[]>> bytesMapSetter) {
        return fromPrefix(prefix.getBytes(StandardCharsets.UTF_8), bytesMapSetter);
    }

    /**
     * Sets up the parsing of a set of columns with the given prefix
     *
     * @param prefix    cells qualifier prefix UTF-8 string
     * @param setter    lambda to populate the result object with converted data from the given prefixes
     * @param converter converts the column bytes to a proper value. This converter will receive the bytes map
     *                  without the qualifier prefix
     * @param <U>       converted value type
     * @return this builder
     */
    public <U> HBaseResultParserSchemaBuilder<T> fromPrefix(String prefix,
                                                            BiConsumer<T, U> setter,
                                                            Function<NavigableMap<byte[], byte[]>, U> converter) {
        return fromPrefix(prefix.getBytes(StandardCharsets.UTF_8), setter, converter);
    }

    /**
     * Sets up the parsing of a set of columns with the given prefix
     *
     * @param prefix    cells qualifier prefix UTF-8 string
     * @param setter    lambda to populate the result object with converted data from the given prefixes
     * @param converter converts the column bytes to a proper value. This converter will receive the bytes map
     *                  without the qualifier prefix
     * @param <U>       converted value type
     * @return this builder
     */
    public <U> HBaseResultParserSchemaBuilder<T> fromPrefix(String prefix,
                                                            BiConsumer<T, U> setter,
                                                            BytesMapConverter<U> converter) {
        return fromPrefix(prefix.getBytes(StandardCharsets.UTF_8), setter, converter);
    }

    /**
     * Builds a new instance of the result parser
     *
     * @return new result parser instance
     */
    public HBaseResultParserSchema<T> build() {
        return new AbstractHBaseResultParserSchema<T>() {
            @Override
            public T newInstance() {
                return constructor.get();
            }

            @Override
            public void setFromRowKey(T obj, byte[] rowKey) {
                fromRowKeySetter.accept(obj, rowKey);
            }

            @Override
            public NavigableSet<byte[]> getPrefixes() {
                return prefixSetters.navigableKeySet();
            }

            @Override
            public void setFromCell(T obj, byte[] qualifier, byte[] value) {
                BiConsumer<T, byte[]> setter = cellSetters.get(qualifier);
                if (setter != null) {
                    setter.accept(obj, value);
                }
            }

            @Override
            public void setFromPrefix(T obj, byte[] prefix, NavigableMap<byte[], byte[]> cellsFromPrefix) {
                BiConsumer<T, NavigableMap<byte[], byte[]>> setter = prefixSetters.get(prefix);
                if (setter != null) {
                    setter.accept(obj, cellsFromPrefix);
                }
            }
        };
    }


}
