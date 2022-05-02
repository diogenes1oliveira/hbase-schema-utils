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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static hbase.schema.api.utils.BytesNullableComparator.BYTES_NULLABLE_COMPARATOR;
import static org.apache.hadoop.hbase.util.Bytes.toStringBinary;

public class HBaseScanRowPaginator<Q, R> extends HBaseFetcherWrapper<Q, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseScanRowPaginator.class);

    private final int pageSize;
    private byte[] startRow;
    private byte[] lastRow = null;

    public HBaseScanRowPaginator(HBaseFetcher<Q, R> fetcher, byte[] startRow, int pageSize) {
        super(fetcher);

        this.pageSize = pageSize;
        setStartRow(startRow);
    }

    public void setStartRow(byte[] startRow) {
        if (startRow == null) {
            this.startRow = null;
        } else {
            this.startRow = Arrays.copyOf(startRow, startRow.length);
        }

        this.lastRow = null;
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

        return Stream.generate(() -> {
            List<Result> allResults = new ArrayList<>();
            LOGGER.info("Starting scans {}", slicer);

            try (Stream<List<Result>> baseStream = super.scan(query, tableName, family, slicer.getScans(), rowBatchSize)) {
                for (Iterator<List<Result>> it = baseStream.iterator(); it.hasNext() && allResults.size() < pageSize; ) {
                    allResults.addAll(it.next());
                }
            }

            return allResults;
        }).limit(1);
    }

    @Override
    public Stream<R> parseResults(Q query, byte[] family, List<Result> hBaseResults) {
        return super.parseResults(query, family, updatePagination(hBaseResults));
    }

    public void reset() {
        this.lastRow = null;
    }

    public ByteBuffer nextRow() {
        if (lastRow == null || BYTES_NULLABLE_COMPARATOR.compare(lastRow, startRow) == 0) {
            return null;
        } else {
            return ByteBuffer.wrap(lastRow).asReadOnlyBuffer();
        }
    }

    private List<Result> updatePagination(List<Result> results) {
        if (results.size() > pageSize) {
            Result last = results.get(results.size() - 1);
            lastRow = last.getRow();
            return results.subList(0, results.size() - 1);
        } else {
            lastRow = null;
            return results;
        }
    }

    @Override
    public String toString() {
        return "HBaseScanRowPaginator{" +
                "pageSize=" + pageSize +
                ", startRow=" + toStringBinary(startRow) +
                ", lastRow=" + toStringBinary(lastRow) +
                '}';
    }
}
