package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.converters.HBaseBytesMapSetter;
import hbase.schema.api.interfaces.converters.HBaseBytesSetter;
import hbase.schema.api.interfaces.HBaseResultParserSchema;

import java.nio.charset.StandardCharsets;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.function.Supplier;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;

/**
 * Builder for {@link HBaseResultParserSchema} objects, providing a fluent API to add fields and field prefixes to be parsed
 *
 * @param <T> result object instance
 */
public class HBaseResultParserBuilder<T> {
    private final Supplier<T> constructor;
    private HBaseBytesSetter<T> fromRowKeySetter = HBaseBytesSetter.dummy();
    private final NavigableMap<byte[], HBaseBytesSetter<T>> cellSetters = asBytesTreeMap();
    private final NavigableMap<byte[], HBaseBytesMapSetter<T>> prefixSetters = asBytesTreeMap();

    /**
     * Instantiate a new builder
     *
     * @param constructor supplier of a new result instance
     */
    public HBaseResultParserBuilder(Supplier<T> constructor) {
        this.constructor = constructor;
    }

    /**
     * Sets up the parsing of the row key
     *
     * @param bytesSetter lambda to populate the result object with data from the row key
     * @return this builder
     */
    public HBaseResultParserBuilder<T> fromRowKey(HBaseBytesSetter<T> bytesSetter) {
        this.fromRowKeySetter = bytesSetter;
        return this;
    }

    /**
     * Sets up the parsing of a fixed column
     *
     * @param qualifier   fixed column qualifier bytes
     * @param bytesSetter lambda to populate the result object with data from the given column
     * @return this builder
     */
    public HBaseResultParserBuilder<T> fromColumn(byte[] qualifier, HBaseBytesSetter<T> bytesSetter) {
        cellSetters.put(qualifier, bytesSetter);
        return this;
    }

    /**
     * Sets up the parsing of a fixed column
     *
     * @param qualifier   fixed column qualifier UTF-8 string
     * @param bytesSetter lambda to populate the result object with binary data from the given column
     * @return this builder
     */
    public HBaseResultParserBuilder<T> fromColumn(String qualifier, HBaseBytesSetter<T> bytesSetter) {
        return fromColumn(qualifier.getBytes(StandardCharsets.UTF_8), bytesSetter);
    }

    /**
     * Sets up the parsing of a set of columns with the given prefix
     *
     * @param prefix         cells qualifier prefix bytes
     * @param bytesMapSetter lambda to populate the result object with data from the given prefixes. This setter
     *                       will receive the data without such prefix
     * @return this builder
     */
    public HBaseResultParserBuilder<T> fromPrefix(byte[] prefix, HBaseBytesMapSetter<T> bytesMapSetter) {
        prefixSetters.put(prefix, bytesMapSetter);
        return this;
    }

    /**
     * Sets up the parsing of a set of columns with the given prefix
     *
     * @param prefix         cells qualifier prefix UTF-8 string
     * @param bytesMapSetter lambda to populate the result object with data from the given prefixes. This setter
     *                       will receive the data without such prefix
     * @return this builder
     */
    public <F> HBaseResultParserBuilder<T> fromPrefix(String prefix, HBaseBytesMapSetter<T> bytesMapSetter) {
        return fromPrefix(prefix.getBytes(StandardCharsets.UTF_8), bytesMapSetter);
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
                fromRowKeySetter.setFromBytes(obj, rowKey);
            }

            @Override
            public NavigableSet<byte[]> getPrefixes() {
                return prefixSetters.navigableKeySet();
            }

            @Override
            public void setFromCell(T obj, byte[] qualifier, byte[] value) {
                HBaseBytesSetter<T> setter = cellSetters.get(qualifier);
                if (setter != null) {
                    setter.setFromBytes(obj, value);
                }
            }

            @Override
            public void setFromPrefix(T obj, byte[] prefix, NavigableMap<byte[], byte[]> cellsFromPrefix) {
                HBaseBytesMapSetter<T> setter = prefixSetters.get(prefix);
                if (setter != null) {
                    setter.setFromBytes(obj, cellsFromPrefix);
                }
            }
        };
    }


}
