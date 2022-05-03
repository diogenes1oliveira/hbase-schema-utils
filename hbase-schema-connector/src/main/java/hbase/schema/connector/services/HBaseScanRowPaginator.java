package hbase.schema.connector.services;

import hbase.schema.connector.interfaces.HBaseFetcher;
import hbase.schema.connector.interfaces.HBaseFetcherWrapper;
import hbase.schema.connector.models.HBaseResultRow;
import hbase.schema.connector.utils.HBaseScansSlicer;
import org.apache.hadoop.hbase.TableName;
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
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.apache.hadoop.hbase.util.Bytes.toStringBinary;

public class HBaseScanRowPaginator<Q, R> extends HBaseFetcherWrapper<Q, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseScanRowPaginator.class);

    private final int pageSize;
    private final Type type;
    private byte[] startRow;
    private byte[] lastRow = null;

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

        this.lastRow = null;
    }

    @Override
    public int defaultRowBatchSize() {
        return pageSize + 1;
    }

    @Override
    public Stream<List<HBaseResultRow>> scan(Q query, TableName tableName, byte[] family, List<Scan> scans, int rowBatchSize) {
        if (rowBatchSize != defaultRowBatchSize()) {
            throw new IllegalArgumentException("Row batch size of " + defaultRowBatchSize() + " required, got " + rowBatchSize);
        }

        HBaseScansSlicer slicer = new HBaseScansSlicer(tableName, scans);
        slicer.removeBefore(startRow);
        slicer.map(scan -> scan.setLimit(rowBatchSize));

        return Stream.generate(() -> {
            List<HBaseResultRow> fetchedRows = new ArrayList<>();
            LOGGER.info("Starting scans {}", slicer);

            try (Stream<List<HBaseResultRow>> baseStream = super.scan(query, tableName, family, slicer.getScans(), rowBatchSize)) {
                for (Iterator<List<HBaseResultRow>> it = baseStream.iterator(); it.hasNext() && fetchedRows.size() < pageSize; ) {
                    fetchedRows.addAll(it.next());
                }
            }

            return fetchedRows;
        }).limit(1);
    }

    @Override
    public Stream<R> parseResults(Q query, List<HBaseResultRow> resultRows) {
        return super.parseResults(query, updatePagination(resultRows));
    }

    public ByteBuffer nextRow() {
        if (lastRow == null || BYTES_NULLABLE_COMPARATOR.compare(lastRow, startRow) == 0) {
            return null;
        } else {
            return ByteBuffer.wrap(lastRow).asReadOnlyBuffer();
        }
    }

    private List<HBaseResultRow> updatePagination(List<HBaseResultRow> resultRows) {
        if (resultRows.size() > pageSize) {
            int newSize;

            if (type == Type.EXACT) {
                newSize = pageSize;
            } else {
                newSize = resultRows.size() - 1;
            }

            lastRow = toBytes(resultRows.get(newSize).rowKey());
            return resultRows.subList(0, newSize);
        } else {
            lastRow = null;
            return resultRows;
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
