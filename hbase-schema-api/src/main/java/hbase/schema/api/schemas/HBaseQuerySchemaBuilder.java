package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseQuerySchema;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.function.Function;

import static hbase.schema.api.utils.HBaseFunctionals.fixedFunction;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;
import static hbase.schema.api.utils.HBaseSchemaUtils.verifyNonNull;
import static java.util.Optional.ofNullable;

public class HBaseQuerySchemaBuilder<T> {
    private Function<T, byte[]> rowKeyGetter = null;
    private Function<T, byte[]> scanKeyGetter = null;
    private Function<T, SortedSet<byte[]>> qualifiersGetter = null;
    private Function<T, SortedSet<byte[]>> prefixesGetter = null;
    @SuppressWarnings({"FieldMayBeFinal", "rawtypes"})
    private static final Function EMPTY = fixedFunction(asBytesTreeSet());

    public HBaseQuerySchemaBuilder<T> withRowKey(Function<T, byte[]> getter) {
        this.rowKeyGetter = getter;
        return this;
    }

    public HBaseQuerySchemaBuilder<T> withScanKey(Function<T, byte[]> getter) {
        this.scanKeyGetter = getter;
        return this;
    }

    public HBaseQuerySchemaBuilder<T> withScanKeySize(int size) {
        return withScanKeySize(query -> size);
    }

    public HBaseQuerySchemaBuilder<T> withScanKeySize(Function<T, Integer> sizeGetter) {
        return withScanKey(query -> {
            byte[] rowKey = rowKeyGetter.apply(query);
            int size = sizeGetter.apply(query);
            return Arrays.copyOfRange(rowKey, 0, size);
        });
    }

    public HBaseQuerySchemaBuilder<T> withQualifiers(byte[] first, byte[]... rest) {
        SortedSet<byte[]> qualifiers = asBytesTreeSet(rest);
        qualifiers.add(first);

        this.qualifiersGetter = query -> qualifiers;
        return this;
    }

    public HBaseQuerySchemaBuilder<T> withQualifiers(String first, String... rest) {
        SortedSet<byte[]> qualifiers = toBytesSet(first, rest);

        this.qualifiersGetter = query -> qualifiers;
        return this;
    }

    public HBaseQuerySchemaBuilder<T> withPrefixes(byte[] first, byte[]... rest) {
        SortedSet<byte[]> prefixes = asBytesTreeSet(rest);
        prefixes.add(first);

        this.prefixesGetter = query -> prefixes;
        return this;
    }

    public HBaseQuerySchemaBuilder<T> withPrefixes(String first, String... rest) {
        SortedSet<byte[]> prefixes = toBytesSet(first, rest);

        this.prefixesGetter = query -> prefixes;
        return this;
    }

    @SuppressWarnings("unchecked")
    public HBaseQuerySchema<T> build() {
        verifyNonNull("no row key generator is set", rowKeyGetter);
        verifyNonNull("needs qualifiers or prefixes", qualifiersGetter, prefixesGetter);
        if (scanKeyGetter == null) {
            scanKeyGetter = rowKeyGetter;
        }

        qualifiersGetter = ofNullable(qualifiersGetter).orElse(EMPTY);
        prefixesGetter = ofNullable(prefixesGetter).orElse(EMPTY);

        return new HBaseQuerySchema<T>() {
            @Override
            public byte[] buildRowKey(T query) {
                return rowKeyGetter.apply(query);
            }

            @Override
            public byte[] buildScanKey(T query) {
                return scanKeyGetter.apply(query);
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

    private static SortedSet<byte[]> toBytesSet(String first, String... rest) {
        SortedSet<byte[]> set = asBytesTreeSet(first.getBytes(StandardCharsets.UTF_8));

        for (String s : rest) {
            set.add(s.getBytes(StandardCharsets.UTF_8));
        }

        return set;
    }
}
