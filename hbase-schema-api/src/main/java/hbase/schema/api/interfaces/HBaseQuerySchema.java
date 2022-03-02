package hbase.schema.api.interfaces;

import java.util.SortedSet;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;

public interface HBaseQuerySchema<T> {
    byte[] buildRowKey(T query);

    default byte[] buildScanKey(T query) {
        return buildRowKey(query);
    }

    default SortedSet<byte[]> getQualifiers(T query) {
        return asBytesTreeSet();
    }

    default SortedSet<byte[]> getPrefixes(T query) {
        return asBytesTreeSet();
    }
}
