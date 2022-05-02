package hbase.connector.services;

import hbase.connector.interfaces.HBaseConnectionProxy;
import hbase.connector.utils.IOExitStack;
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

public class HBaseStreamFetcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseStreamFetcher.class);
    private final HBaseConnector connector;

    public HBaseStreamFetcher(HBaseConnector connector) {
        this.connector = connector;
    }

    public Stream<Result> fetch(TableName tableName, Get get) {
        AtomicInteger count = new AtomicInteger(0);

        return Stream.generate(() -> {
            try (Connection connection = connector.context(); Table table = connection.getTable(tableName)) {
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

    public Stream<Result[]> fetch(TableName tableName, List<Scan> scans, int batchSize) {
        return scans.stream()
                    .flatMap(scan -> fetch(tableName, scan, batchSize));
    }

    public Stream<Result[]> fetch(TableName tableName, Scan scan, int batchSize) {
        IOExitStack exitStack = new IOExitStack("Exit stack for scan " + scan);

        IOLazyRef<HBaseConnectionProxy> connectionRef = new IOLazyRef<>(() -> {
            HBaseConnectionProxy connection = connector.context();
            exitStack.add("failed to close connection context after Scan", connection::close);
            return connection;
        });
        IOLazyRef<Table> tableRef = new IOLazyRef<>(() -> {
            Table table = connectionRef.get().getTable(tableName);
            exitStack.add("failed to close table after Scan", table::close);
            return table;
        });
        IOLazyRef<ResultScanner> scannerRef = new IOLazyRef<>(() -> {
            ResultScanner scanner = tableRef.get().getScanner(scan);
            exitStack.add("failed to close scanner", scanner::close);
            return scanner;
        });

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
        }).onClose(exitStack::close);
    }
}
