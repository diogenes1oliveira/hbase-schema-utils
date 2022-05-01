package hbase.schema.connector.interfaces;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public abstract class HBaseFetcherWrapper<Q, R> implements HBaseFetcher<Q, R> {
    private final HBaseFetcher<Q, R> wrapped;

    public HBaseFetcherWrapper(HBaseFetcher<Q, R> wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public Get toGet(Q query) {
        return wrapped.toGet(query);
    }

    @Override
    public Stream<R> get(Q query, TableName tableName, byte[] family, Get get) {
        return wrapped.get(query, tableName, family, get);
    }

    @Override
    public final Stream<R> get(Q query, String tableName, String family, Get get) {
        return HBaseFetcher.super.get(query, tableName, family, get);
    }

    @Override
    public List<Scan> toScans(Q query) {
        return wrapped.toScans(query);
    }

    @Override
    public Stream<Result> scan(Q query, TableName tableName, byte[] family, List<Scan> scans) {
        return wrapped.scan(query, tableName, family, scans);
    }

    @Override
    public final Stream<R> scan(Q query, TableName tableName, byte[] family) {
        return HBaseFetcher.super.scan(query, tableName, family);
    }

    @Override
    public final Stream<Result> scan(Q query, String tableName, String family, List<Scan> scans) {
        return HBaseFetcher.super.scan(query, tableName, family, scans);
    }

    @Override
    public final Stream<R> scan(Q query, String tableName, String family) {
        return HBaseFetcher.super.scan(query, tableName, family);
    }

    @Override
    public Optional<R> parseResult(Q query, byte[] family, Result hBaseResult) {
        return wrapped.parseResult(query, family, hBaseResult);
    }


}
