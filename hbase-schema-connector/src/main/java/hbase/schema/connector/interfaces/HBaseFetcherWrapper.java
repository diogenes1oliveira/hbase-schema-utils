package hbase.schema.connector.interfaces;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
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
    public Stream<Result> get(Q query, TableName tableName, byte[] family, Get get) {
        return wrapped.get(query, tableName, family, get);
    }

    @Override
    public List<Scan> toScans(Q query) {
        return wrapped.toScans(query);
    }

    @Override
    public int defaultRowBatchSize() {
        return wrapped.defaultRowBatchSize();
    }

    @Override
    public Stream<Result[]> scan(Q query, TableName tableName, byte[] family, List<Scan> scans, int rowBatchSize) {
        return wrapped.scan(query, tableName, family, scans, rowBatchSize);
    }

    @Override
    public Stream<R> parseResults(Q query, byte[] family, Stream<Result> hBaseResults) {
        return wrapped.parseResults(query, family, hBaseResults);
    }

    @Override
    public final Stream<Result> get(Q query, String tableName, String family, Get get) {
        return HBaseFetcher.super.get(query, tableName, family, get);
    }

    /**
     * Builds, executes and parses Get requests
     *
     * @param query     query object
     * @param tableName table to execute Get into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    @Override
    public final Optional<R> getOptional(Q query, TableName tableName, byte[] family) {
        return HBaseFetcher.super.getOptional(query, tableName, family);
    }

    /**
     * Builds, executes and parses Get requests
     *
     * @param query     query object
     * @param tableName table to execute Get into
     * @param family    column family to fetch data from
     * @return stream of valid parsed results
     */
    @Override
    public final Optional<R> getOptional(Q query, String tableName, String family) {
        return HBaseFetcher.super.getOptional(query, tableName, family);
    }

    @Override
    public final Stream<Result[]> scan(Q query, String tableName, String family, List<Scan> scans, int rowBatchSize) {
        return HBaseFetcher.super.scan(query, tableName, family, scans, rowBatchSize);
    }

    @Override
    public final Stream<List<R>> scan(Q query, TableName tableName, byte[] family, int rowBatchSize) {
        return HBaseFetcher.super.scan(query, tableName, family, rowBatchSize);
    }

    @Override
    public final Stream<List<R>> scan(Q query, String tableName, String family, int rowBatchSize) {
        return HBaseFetcher.super.scan(query, tableName, family, rowBatchSize);
    }

    @Override
    public final List<R> scanList(Q query, TableName tableName, byte[] family, int rowBatchSize) throws IOException {
        return HBaseFetcher.super.scanList(query, tableName, family, rowBatchSize);
    }

    @Override
    public final List<R> scanList(Q query, String tableName, String family, int rowBatchSize) throws IOException {
        return HBaseFetcher.super.scanList(query, tableName, family, rowBatchSize);
    }

    @Override
    public final Stream<Result[]> scan(Q query, String tableName, String family, List<Scan> scans) {
        return HBaseFetcher.super.scan(query, tableName, family, scans);
    }

    @Override
    public final Stream<List<R>> scan(Q query, TableName tableName, byte[] family) {
        return HBaseFetcher.super.scan(query, tableName, family);
    }

    @Override
    public final Stream<List<R>> scan(Q query, String tableName, String family) {
        return HBaseFetcher.super.scan(query, tableName, family);
    }

    @Override
    public final List<R> scanList(Q query, TableName tableName, byte[] family) throws IOException {
        return HBaseFetcher.super.scanList(query, tableName, family);
    }

    @Override
    public final List<R> scanList(Q query, String tableName, String family) throws IOException {
        return HBaseFetcher.super.scanList(query, tableName, family);
    }

    /**
     * Parses data incoming from a HBase query
     *
     * @param query        original query object
     * @param family       column family the result came from
     * @param hBaseResults HBase result objects
     * @return Stream with the valid parsed results
     */
    @Override
    public final Stream<R> parseResults(Q query, String family, Stream<Result> hBaseResults) {
        return HBaseFetcher.super.parseResults(query, family, hBaseResults);
    }

    /**
     * Parses data incoming from a HBase query
     *
     * @param query       original query object
     * @param family      column family the result came from
     * @param hBaseResult HBase result object
     * @return Optional with the parsed result or empty if nothing could be parsed
     */
    @Override
    public final Optional<R> parseResult(Q query, byte[] family, Result hBaseResult) {
        return HBaseFetcher.super.parseResult(query, family, hBaseResult);
    }

    /**
     * Parses data incoming from a HBase query
     *
     * @param query       original query object
     * @param family      column family the result came from
     * @param hBaseResult HBase result object
     * @return Optional with the parsed result or empty if nothing could be parsed
     */
    @Override
    public final Optional<R> parseResult(Q query, String family, Result hBaseResult) {
        return HBaseFetcher.super.parseResult(query, family, hBaseResult);
    }
}
