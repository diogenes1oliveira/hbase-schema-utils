package hbase.schema.connector.services;

import hbase.connector.services.HBaseConnector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class HBaseStreamGetter {
    private final HBaseConnector connector;

    public HBaseStreamGetter(HBaseConnector connector) {
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
}
