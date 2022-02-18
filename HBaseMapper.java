package com.github.diogenes1oliveira.hbase.schema;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public interface HBaseMapper<T> extends HBaseResultMapper<T>, HBaseMutationsMapper<T, Mutation> {
    default Get get(byte[] family, T obj) {
        byte[] rowKey = toRowKey(family, obj)
                .orElseThrow(() -> new IllegalArgumentException("No row key for object"));

        Get get = new Get(rowKey);
        byte[] prefix = prefix(family).orElse(null);

        if (prefix != null) {
            get.addFamily(family);
        } else {
            for (byte[] qualifier : qualifiers(family, obj)) {
                get.addColumn(family, qualifier);
            }
        }

        return get;
    }

    default Scan scan(byte[] family, int searchKeySize, List<T> queries) {
        byte[] rowKey = toRowKey(family, obj)
                .orElseThrow(() -> new IllegalArgumentException("No row key for object"));
        byte[] searchKey = Arrays.copyOf(rowKey, searchKeySize);

        Scan scan = new Scan();
        byte[] prefix = prefix(family).orElse(null);

        if (prefix != null) {
            scan.addFamily(family);
        } else {
            for (byte[] qualifier : qualifiers(family, obj)) {
                scan.addColumn(family, qualifier);
            }
        }

        return scan;
    }

    default Optional<Filter> prefixFilter(byte[] family) {
        byte[] prefix = prefix(family).orElse(null);
        if (prefix == null) {
            return Optional.empty();
        }
        return Optional.of(new ColumnPrefixFilter(prefix));
    }
}
