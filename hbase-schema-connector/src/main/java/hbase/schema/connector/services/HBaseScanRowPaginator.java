package hbase.schema.connector.services;

import hbase.schema.connector.interfaces.HBaseFetcher;
import hbase.schema.connector.interfaces.HBaseFetcherWrapper;
import hbase.schema.connector.models.HBasePaginatedQuery;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static hbase.schema.api.utils.ByteBufferComparator.BYTE_BUFFER_COMPARATOR;
import static hbase.schema.api.utils.ScanComparator.SCAN_COMPARATOR;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.apache.hadoop.hbase.util.Bytes.toStringBinary;

public class HBaseScanRowPaginator<Q, R> extends HBaseFetcherWrapper<Q, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseScanRowPaginator.class);

    private final HBasePaginatedQuery paginatedQuery;
    private final AtomicInteger resultCount = new AtomicInteger(0);
    private final AtomicReference<byte[]> resultRowRef = new AtomicReference<>();

    public HBaseScanRowPaginator(HBaseFetcher<Q, R> fetcher, HBasePaginatedQuery paginatedQuery) {
        super(fetcher);

        this.paginatedQuery = paginatedQuery;
    }

    @Override
    public List<Scan> toScans(Q query) {
        LOGGER.info("Calling super");
        List<Scan> scans = super.toScans(query);

        int limit = paginatedQuery.getPageSize();
        for (Scan scan : scans) {
            scan.setLimit(limit);
        }
        LOGGER.info("Remapping {} old scans: {}", scans.size(), scans);

        ByteBuffer nextRow = paginatedQuery.getNextRow();
        if (nextRow == null || !nextRow.hasRemaining()) {
            LOGGER.info("Next row not set in {}", paginatedQuery);
            return scans;
        }
        List<Scan> newScans = scans.stream()
                                   .filter(scan -> isAfter(scan, nextRow))
                                   .map(scan -> checkBounds(scan, nextRow))
                                   .sorted(SCAN_COMPARATOR)
                                   .collect(toList());


        LOGGER.info("Generated {} new scans: {}", newScans.size(), newScans);
        return newScans;
    }

    @Override
    public Optional<R> parseResult(Q query, byte[] family, Result hBaseResult) {
        resultRowRef.set(hBaseResult.getRow());
        LOGGER.info("Setting result row ref to {}, now count = {}", toStringBinary(resultRowRef.get()), resultCount.incrementAndGet());
        return super.parseResult(query, family, hBaseResult);
    }

    @Override
    public Stream<Result> scan(Q query, TableName tableName, byte[] family, List<Scan> scans) {
        paginatedQuery.setNextRow(null);
        LOGGER.info("paginatedQuery now is {}", paginatedQuery);

        return super.scan(query, tableName, family, scans).onClose(() -> {
            LOGGER.info("result row ref now is {}", toStringBinary(resultRowRef.get()));
            if (resultCount.get() >= paginatedQuery.getPageSize()) {
                paginatedQuery.setNextRow(resultRowRef.get());
            }
        });
    }

    private static boolean isAfter(Scan scan, ByteBuffer nextRow) {
        byte[] scanStop = scan.getStopRow();
        if (scanStop == null) {
            LOGGER.info("Scan {} is after next row {}", scan, toStringBinary(nextRow));
            return true;
        } else if (BYTE_BUFFER_COMPARATOR.compare(ByteBuffer.wrap(scanStop), nextRow) > 0) {
            LOGGER.info("Scan {} is after next row {}", scan, toStringBinary(nextRow));
            return true;
        } else {
            LOGGER.info("Scan {} is before next row {}", scan, toStringBinary(nextRow));
            return false;
        }
    }

    private static Scan checkBounds(Scan scan, ByteBuffer nextRow) {
        byte[] scanStartBytes = scan.getStartRow();
        String oldScan = scan.toString();

        if (scanStartBytes == null) {
            Scan newScan = scan.withStartRow(toBytes(nextRow));
            LOGGER.info("Old scan {} reframed to {}", oldScan, newScan);
            return newScan;
        }
        ByteBuffer scanStart = ByteBuffer.wrap(scanStartBytes);
        if (BYTE_BUFFER_COMPARATOR.compare(scanStart, nextRow) < 0) {
            Scan newScan = scan.withStartRow(toBytes(nextRow));
            LOGGER.info("Old scan {} reframed to {}", oldScan, newScan);
            return newScan;
        }

        LOGGER.info("Old scan {} does not need to be reframed", scan);
        return scan;
    }
}
