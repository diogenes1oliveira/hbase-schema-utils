package hbase.schema.connector.services;

import hbase.schema.connector.interfaces.HBaseFetcher;
import hbase.schema.connector.interfaces.HBaseFetcherWrapper;
import hbase.schema.connector.utils.HBaseScansSlicer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static hbase.connector.utils.TakeWhileIterator.streamTakeWhile;
import static java.util.Optional.ofNullable;
import static org.apache.hadoop.hbase.util.Bytes.toStringBinary;

public class HBaseScanRowPaginator<Q, R> extends HBaseFetcherWrapper<Q, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseScanRowPaginator.class);

    private final int rowBatchSize;
    private byte[] startRow;
    private int resultCount = 0;
    private byte[] nextRow = null;

    public HBaseScanRowPaginator(HBaseFetcher<Q, R> fetcher, byte[] startRow, int pageSize) {
        super(fetcher);

        this.rowBatchSize = pageSize + 1;

        setStartRow(startRow);
    }

    public void setStartRow(byte[] startRow) {
        if (startRow == null) {
            this.startRow = null;
        } else {
            this.startRow = Arrays.copyOf(startRow, startRow.length);
        }
    }

    @Override
    public int defaultRowBatchSize() {
        return rowBatchSize;
    }

    @Override
    public Stream<Result[]> scan(Q query, TableName tableName, byte[] family, List<Scan> scans, int rowBatchSize) {
        if (rowBatchSize != this.rowBatchSize) {
            throw new IllegalArgumentException("Row batch size of " + this.rowBatchSize + " required, got " + rowBatchSize);
        }

        HBaseScansSlicer slicer = new HBaseScansSlicer(tableName, scans);
        slicer.removeBefore(startRow);
        List<Scan> newScans = slicer.getScans().stream().map(scan -> scan.setLimit(rowBatchSize)).collect(Collectors.toList());

        LOGGER.info("Starting scans {}", newScans);
        return streamTakeWhile(
                super.scan(query, tableName, family, newScans, rowBatchSize),
                results -> resultCount < rowBatchSize
        ).onClose(this::checkNextRow).peek(this::updatePagination);
    }

    @Override
    public Stream<R> parseResults(Q query, byte[] family, Stream<Result> hBaseResults) {
        return super.parseResults(query, family, hBaseResults).limit(rowBatchSize - 1);
    }

    private void checkNextRow() {
        if (nextRow != null && startRow != null && Bytes.compareTo(nextRow, startRow) == 0) {
            nextRow = null;
        }
        this.resultCount = 0;
    }

    private void updatePagination(Result[] hBaseResults) {
        resultCount += hBaseResults.length;

        if (hBaseResults.length > 0) {
            nextRow = null;
            if (hBaseResults.length >= rowBatchSize) {
                Result last = hBaseResults[hBaseResults.length - 1];
                if (last != null && last.getRow() != null) {
                    nextRow = last.getRow();
                }
            }
        }

        LOGGER.info("New next row: {}", toStringBinary(nextRow));
    }

    public ByteBuffer nextRow() {
        return ofNullable(nextRow).map(ByteBuffer::wrap).map(ByteBuffer::asReadOnlyBuffer).orElse(null);
    }

}
