package hbase.schema.connector.services;

import hbase.schema.connector.interfaces.HBaseFetcher;
import hbase.schema.connector.interfaces.HBaseFetcherWrapper;
import hbase.schema.connector.utils.HBaseScansSlicer;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static hbase.connector.utils.TakeWhileIterator.streamTakeWhile;
import static hbase.schema.api.utils.BytesNullableComparator.BYTES_NULLABLE_COMPARATOR;
import static org.apache.hadoop.hbase.util.Bytes.toStringBinary;

public class HBaseScanRowPaginator<Q, R> extends HBaseFetcherWrapper<Q, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseScanRowPaginator.class);

    private final int pageSize;
    private final Type type;
    private byte[] startRow;
    private int resultCount = 0;
    private byte[] nextRow = null;

    public enum Type {
        DESIRED,
        EXACT
    }

    public HBaseScanRowPaginator(HBaseFetcher<Q, R> fetcher, byte[] startRow, int pageSize) {
        this(fetcher, startRow, pageSize, Type.DESIRED);
    }

    public HBaseScanRowPaginator(HBaseFetcher<Q, R> fetcher, byte[] startRow, int pageSize, Type type) {
        super(fetcher);

        this.pageSize = pageSize;
        this.type = type;
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
        return pageSize + 1;
    }

    @Override
    public Stream<List<Result>> scan(Q query, TableName tableName, byte[] family, List<Scan> scans, int rowBatchSize) {
        if (rowBatchSize != defaultRowBatchSize()) {
            throw new IllegalArgumentException("Row batch size of " + defaultRowBatchSize() + " required, got " + rowBatchSize);
        }

        HBaseScansSlicer slicer = new HBaseScansSlicer(tableName, scans);
        slicer.removeBefore(startRow);
        slicer.map(scan -> scan.setLimit(rowBatchSize));

        LOGGER.info("Starting scans {}", slicer);
        return streamTakeWhile(
                super.scan(query, tableName, family, slicer.getScans(), rowBatchSize),
                results -> resultCount < pageSize
        ).peek(this::updatePagination);
    }

    public void reset() {
        this.resultCount = 0;
    }

    public ByteBuffer nextRow() {
        if (nextRow == null || BYTES_NULLABLE_COMPARATOR.compare(nextRow, startRow) == 0) {
            return null;
        } else {
            return ByteBuffer.wrap(nextRow).asReadOnlyBuffer();
        }
    }

    private void updatePagination(List<Result> hBaseResults) {
        resultCount += hBaseResults.size();

        if (hBaseResults.size() > 0) {
            if (hBaseResults.size() >= pageSize) {
                Result last = hBaseResults.get(hBaseResults.size() - 1);
                if (last != null && last.getRow() != null) {
                    nextRow = last.getRow();
                }
            }
        }

        LOGGER.info("New next row: {}", toStringBinary(nextRow));
    }

    @Override
    public String toString() {
        return "HBaseScanRowPaginator{" +
                "pageSize=" + pageSize +
                ", type=" + type +
                ", startRow=" + toStringBinary(startRow) +
                ", resultCount=" + resultCount +
                ", nextRow=" + toStringBinary(nextRow) +
                '}';
    }
}
