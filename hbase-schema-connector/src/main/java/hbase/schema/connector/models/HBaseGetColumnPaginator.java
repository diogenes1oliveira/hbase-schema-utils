package hbase.schema.connector.models;

import hbase.schema.api.interfaces.HBaseFilterBuilder;
import hbase.schema.connector.interfaces.HBaseFetcher;
import hbase.schema.connector.interfaces.HBaseFetcherWrapper;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.Filter;

import java.util.NavigableMap;
import java.util.stream.Stream;

import static hbase.schema.api.utils.HBaseSchemaUtils.combineNullableFilters;

public class HBaseGetColumnPaginator<Q, R> extends HBaseFetcherWrapper<Q, R> implements HBaseFilterBuilder<Q> {
    private final int pageSize;
    private int pageIndex;
    private int nextPageIndex = 0;

    public HBaseGetColumnPaginator(HBaseFetcher<Q, R> fetcher, int pageSize, int pageIndex) {
        super(fetcher);

        this.pageIndex = pageIndex;
        this.pageSize = pageSize;
    }

    public HBaseGetColumnPaginator(HBaseFetcher<Q, R> fetcher, int pageSize) {
        this(fetcher, pageSize, 0);
    }

    public void setPageIndex(int pageIndex) {
        this.pageIndex = pageIndex;
        this.nextPageIndex = 0;
    }

    public int nextPageIndex() {
        return nextPageIndex;
    }

    @Override
    public Stream<Result> get(Q query, TableName tableName, byte[] family, Get get) {
        Get newGet = get;
        Filter combined = combineNullableFilters(get.getFilter(), toFilter(query));
        if (combined != null) {
            newGet = newGet.setFilter(combined);
        }
        return super.get(query, tableName, family, newGet)
                    .peek(result -> checkResultSize(result, family));
    }

    @Override
    public Filter toFilter(Q query) {
        return new ColumnPaginationFilter(pageSize + 1, pageSize * pageIndex);
    }

    @Override
    public String toString() {
        return "HBaseGetColumnPaginator{" +
                "pageSize=" + pageSize +
                ", pageIndex=" + pageIndex +
                ", nextPageIndex=" + nextPageIndex +
                '}';
    }

    private void checkResultSize(Result result, byte[] family) {
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(family);
        if (familyMap.size() <= pageSize) {
            nextPageIndex = 0;
        } else {
            nextPageIndex = pageIndex + 1;
            familyMap.remove(familyMap.lastKey());
        }
    }
}
