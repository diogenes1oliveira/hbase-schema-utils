package hbase.connector.services;

import hbase.connector.utils.IOLazyRef;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static hbase.connector.utils.TakeWhileIterator.streamTakeWhile;

/**
 * Object to supply a nice stream-based API to fetch data from HBase
 */
public class HBaseStreamFetcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseStreamFetcher.class);
    private final HBaseConnector connector;

    /**
     * @param connector HBase connector object
     */
    public HBaseStreamFetcher(HBaseConnector connector) {
        this.connector = connector;
    }

    /**
     * Yields the result of a Get as a single Java stream
     *
     * @param tableName name of the table to be fetched
     * @param get       Get object
     * @param userName  username for the connection
     * @return stream that yields one or no results
     * @throws UncheckedIOException failed to connect or fetch data from HBase
     */
    public Stream<Result> get(String tableName, Get get, String userName) {
        AtomicInteger count = new AtomicInteger(0);

        return Stream.generate(() -> {
            Connection connection = connector.get(userName);

            try (Table table = connection.getTable(TableName.valueOf(tableName))) {
                Result result = table.get(get);

                if (result != null && result.getRow() != null) {
                    if (count.getAndIncrement() > 0) {
                        throw new IllegalStateException("Should have run just once");
                    }
                    return result;
                } else {
                    return null;
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).limit(1).filter(Objects::nonNull);
    }

    /**
     * Yields the results of multiple Scans sequentially as a single Java stream
     *
     * @param tableName name of the table to be scanned
     * @param scans     Scan objects
     * @param userName  username for the connection
     * @param batchSize maximum size of each result batch
     * @return stream that yields a batch of results at each iteration. MUST be closed after completion
     * @throws UncheckedIOException failed to connect or fetch data from HBase
     */
    public Stream<Result[]> scan(String tableName, List<Scan> scans, String userName, int batchSize) {
        return scans.stream()
                    .flatMap(scan -> this.scan(tableName, scan, userName, batchSize));
    }

    /**
     * Yields the Scan results as a Java stream
     *
     * @param tableName name of the table to be scanned
     * @param scan      Scan object
     * @param userName  username for the connection
     * @param batchSize maximum size of each result batch
     * @return stream that yields a batch of results at each iteration. MUST be closed after completion
     * @throws UncheckedIOException failed to connect or fetch data from HBase
     */
    public Stream<Result[]> scan(String tableName, Scan scan, String userName, int batchSize) {
        IOLazyRef<Table> tableRef = new IOLazyRef<>(() -> {
            Connection connection = connector.connect(userName);
            return connection.getTable(TableName.valueOf(tableName));
        });
        IOLazyRef<ResultScanner> scannerRef = new IOLazyRef<>(() -> tableRef.get().getScanner(scan));

        Stream<Result[]> stream = Stream.generate(() -> {
            try {
                LOGGER.debug("StreamFetcher: fetching batch from scanner");
                return scannerRef.get().next(batchSize);
            } catch (IOException e) {
                LOGGER.warn("StreamFetcher: failed to fetch batch from scanner", e);
                throw new UncheckedIOException(e);
            }
        });

        return streamTakeWhile(stream, results -> {
            if (results.length > 0) {
                return true;
            } else {
                LOGGER.debug("StreamFetcher: no results, stream should be finished");
                return false;
            }
        }).onClose(() -> {
            try {
                scannerRef.remove(ResultScanner::close);
            } catch (UncheckedIOException scannerException) {
                try {
                    tableRef.remove(Table::close);
                } catch (UncheckedIOException tableException) {
                    LOGGER.warn("Failed cleaning up table object after scanner failure", tableException);
                }
                throw scannerException;
            }
        });
    }

}
