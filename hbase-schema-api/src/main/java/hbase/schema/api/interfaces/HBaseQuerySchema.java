package hbase.schema.api.interfaces;

import edu.umd.cs.findbugs.annotations.Nullable;
import hbase.schema.api.utils.HBaseSchemaUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import java.util.List;
import java.util.SortedSet;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;
import static hbase.schema.api.utils.HBaseSchemaUtils.toMultiRowRangeFilter;
import static java.util.Objects.requireNonNull;

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

    default void selectColumns(T query, Get get, byte[] family) {
        HBaseSchemaUtils.selectColumns(get, family, getQualifiers(query), getPrefixes(query));
    }

    default void selectColumns(T query, Scan scan, byte[] family) {
        HBaseSchemaUtils.selectColumns(scan, family, getQualifiers(query), getPrefixes(query));
    }

    default Get toGet(T query, byte[] family) {
        byte[] rowKey = requireNonNull(buildRowKey(query));
        Get get = new Get(rowKey);
        selectColumns(query, get, family);

        Filter filter = toFilter(query);
        if (filter != null) {
            get.setFilter(filter);
        }
        return get;
    }

    default Scan toScan(List<? extends T> queries, byte[] family) {
        Scan scan = new Scan();
        selectColumns(queries.get(0), scan, family);

        Filter filter = toFilter(queries);
        if (filter != null) {
            scan.setFilter(filter);
        }
        return scan;
    }

    @Nullable
    default Filter toFilter(T query) {
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (byte[] prefix : getPrefixes(query)) {
            if (prefix.length > 0) {
                Filter qualifierFilter = new ColumnPrefixFilter(prefix);
                list.addFilter(qualifierFilter);
            }
        }
        if (list.size() == 0) {
            return null;
        } else {
            return list;
        }
    }

    default Filter toFilter(List<? extends T> queries) {
        return toMultiRowRangeFilter(queries
                .stream()
                .map(this::buildScanKey)
                .iterator()
        );
    }
}
