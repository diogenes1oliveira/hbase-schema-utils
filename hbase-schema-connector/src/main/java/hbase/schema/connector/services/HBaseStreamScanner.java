package hbase.schema.connector.services;

import hbase.connector.interfaces.HBaseConnectionProxy;
import hbase.connector.services.HBaseConnector;
import hbase.schema.connector.utils.IOExitStack;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.function.UnaryOperator.identity;

public class HBaseStreamScanner {
    private final HBaseConnector connector;

    public HBaseStreamScanner(HBaseConnector connector) {
        this.connector = connector;
    }

    public Stream<Result> fetch(TableName tableName, List<Scan> scans) {
        return scans.stream()
                    .flatMap(scan -> fetch(tableName, scan));
    }

    public Stream<Result> fetch(TableName tableName, Scan scan) {
        return Stream.generate(() -> {
            IOExitStack exitStack = new IOExitStack();

            try {
                HBaseConnectionProxy connection = connector.context();
                exitStack.add("failed to close connection context after Scan", connection::close);

                Table table = connection.getTable(tableName);
                exitStack.add("failed to close table after Scan", table::close);

                ResultScanner scanner = table.getScanner(scan);
                exitStack.add("failed to close scanner", scanner::close);

                return StreamSupport.stream(scanner.spliterator(), false)
                                    .filter(result -> result != null && result.getRow() != null)
                                    .onClose(exitStack::close);
            } catch (IOException e) {
                try {
                    throw new UncheckedIOException(e);
                } finally {
                    exitStack.close();
                }
            } catch (RuntimeException e) {
                try {
                    throw e;
                } finally {
                    exitStack.close();
                }
            }

        }).limit(1).flatMap(identity());
    }
}
