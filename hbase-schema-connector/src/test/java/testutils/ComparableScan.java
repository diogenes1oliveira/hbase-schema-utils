package testutils;

import hbase.schema.api.utils.ScanComparator;
import org.apache.hadoop.hbase.client.Scan;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class ComparableScan extends Scan implements Comparable<Scan> {
    ComparableScan(Scan scan) throws IOException {
        super(scan);
    }

    @Override
    public int compareTo(@NotNull Scan other) {
        return ScanComparator.SCAN_COMPARATOR.compare(this, other);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Scan)) {
            return false;
        }
        Scan other = (Scan) o;
        return this.compareTo(other) == 0;
    }


    public static ComparableScan comparableScan(Scan scan) {
        try {
            return new ComparableScan(scan);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static List<ComparableScan> comparableScans(List<Scan> scans) {
        return scans.stream().map(ComparableScan::comparableScan).collect(toList());
    }
}
