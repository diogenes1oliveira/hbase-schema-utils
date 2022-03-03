package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseQuerySchema;
import hbase.schema.api.interfaces.converters.HBaseBytesGetter;
import hbase.schema.api.interfaces.converters.HBaseIntegerGetter;
import hbase.schema.api.utils.HBaseSchemaConversions;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.function.Function;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;
import static hbase.schema.api.utils.HBaseSchemaConversions.utf8ToBytes;
import static hbase.schema.api.utils.HBaseSchemaUtils.verifyNonNull;
import static java.util.Optional.ofNullable;

/**
 * Builder for {@link HBaseQuerySchema} objects, providing a fluent API to add fields and field prefixes to be fetched
 *
 * @param <T> query object instance
 */
public class HBaseQuerySchemaBuilder<T> {
    private HBaseBytesGetter<T> rowKeyGetter = null;
    private HBaseBytesGetter<T> scanKeyGetter = null;
    private Function<T, SortedSet<byte[]>> qualifiersGetter = null;
    private Function<T, SortedSet<byte[]>> prefixesGetter = null;

    /**
     * Generates the row key to be used in a Get request
     *
     * @param getter lambda to build a Get row key from the object
     * @return this builder
     */
    public HBaseQuerySchemaBuilder<T> withRowKey(HBaseBytesGetter<T> getter) {
        this.rowKeyGetter = getter;
        return this;
    }

    /**
     * Generates the search key to be used in a Scan request
     *
     * @param getter lambda to build a Scan search key from the object
     * @return this builder
     */
    public HBaseQuerySchemaBuilder<T> withScanKey(HBaseBytesGetter<T> getter) {
        this.scanKeyGetter = getter;
        return this;
    }

    /**
     * Sets the search key size to be used in a Scan request
     *
     * @param size search key size
     * @return this builder
     */
    public HBaseQuerySchemaBuilder<T> withScanKeySize(int size) {
        return withScanKeySize(query -> size);
    }

    /**
     * Sets the search key size to be used in a Scan request
     *
     * @param sizeGetter lambda to get the Scan search key size from the object
     * @return this builder
     */
    public HBaseQuerySchemaBuilder<T> withScanKeySize(HBaseIntegerGetter<T> sizeGetter) {
        return withScanKey(query -> {
            byte[] rowKey = rowKeyGetter.getBytes(query);
            Integer size = sizeGetter.getInteger(query);
            if (rowKey == null || size == null) {
                return null;
            }
            return Arrays.copyOfRange(rowKey, 0, size);
        });
    }

    /**
     * Sets the fixed qualifiers to be fetched in the query results
     *
     * @param first fixed {@code byte[]} qualifier
     * @param rest  other qualifiers
     * @return this builder
     */
    public HBaseQuerySchemaBuilder<T> withQualifiers(byte[] first, byte[]... rest) {
        SortedSet<byte[]> qualifiers = asBytesTreeSet(rest);
        qualifiers.add(first);

        this.qualifiersGetter = query -> qualifiers;
        return this;
    }

    /**
     * Sets the fixed qualifiers to be fetched in the query results
     *
     * @param first fixed UTF-8 qualifier
     * @param rest  other qualifiers
     * @return this builder
     */
    public HBaseQuerySchemaBuilder<T> withQualifiers(String first, String... rest) {
        return withQualifiers(utf8ToBytes(first), utf8ToBytesArray(rest));
    }

    /**
     * Sets the qualifier prefixes to be fetched in the query results
     *
     * @param first fixed {@code byte[]} prefix
     * @param rest  other prefixes
     * @return this builder
     */
    public HBaseQuerySchemaBuilder<T> withPrefixes(byte[] first, byte[]... rest) {
        SortedSet<byte[]> prefixes = asBytesTreeSet(rest);
        prefixes.add(first);

        this.prefixesGetter = query -> prefixes;
        return this;
    }

    /**
     * Sets the qualifier prefixes to be fetched in the query results
     *
     * @param first fixed UTF-8 prefix
     * @param rest  other prefixes
     * @return this builder
     */
    public HBaseQuerySchemaBuilder<T> withPrefixes(String first, String... rest) {
        return withPrefixes(utf8ToBytes(first), utf8ToBytesArray(rest));
    }

    /**
     * Builds a new instance of the query schema
     *
     * @return new query schema instance
     */
    public HBaseQuerySchema<T> build() {
        verifyNonNull("no row key generator is set", rowKeyGetter);
        verifyNonNull("needs qualifiers or prefixes", qualifiersGetter, prefixesGetter);
        if (scanKeyGetter == null) {
            scanKeyGetter = rowKeyGetter;
        }

        qualifiersGetter = ofNullable(qualifiersGetter).orElse(obj -> asBytesTreeSet());
        prefixesGetter = ofNullable(prefixesGetter).orElse(obj -> asBytesTreeSet());

        return new HBaseQuerySchema<T>() {
            @Override
            public byte[] buildRowKey(T query) {
                return rowKeyGetter.getBytes(query);
            }

            @Override
            public byte[] buildScanKey(T query) {
                return scanKeyGetter.getBytes(query);
            }

            @Override
            public SortedSet<byte[]> getQualifiers(T query) {
                return qualifiersGetter.apply(query);
            }

            @Override
            public SortedSet<byte[]> getPrefixes(T query) {
                return prefixesGetter.apply(query);
            }
        };
    }

    private static byte[][] utf8ToBytesArray(String... strings) {
        return Arrays.stream(strings)
                     .map(HBaseSchemaConversions::utf8ToBytes)
                     .toArray(byte[][]::new);
    }
}
