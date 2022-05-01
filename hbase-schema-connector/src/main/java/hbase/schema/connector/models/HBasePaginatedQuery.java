package hbase.schema.connector.models;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static java.util.Optional.ofNullable;
import static org.apache.hadoop.hbase.util.Bytes.toStringBinary;

public class HBasePaginatedQuery {
    private final int pageSize;
    private ByteBuffer nextRow;

    public HBasePaginatedQuery(int pageSize, ByteBuffer nextRow) {
        this.pageSize = pageSize;
        this.nextRow = nextRow;
    }

    public HBasePaginatedQuery(int pageSize, byte[] nextRow) {
        this(pageSize, ofNullable(nextRow).map(ByteBuffer::wrap).orElse(null));
    }

    public HBasePaginatedQuery(int pageSize, String nextRow) {
        this(pageSize, ofNullable(nextRow).map(s -> s.getBytes(StandardCharsets.UTF_8)).orElse(null));
    }

    public ByteBuffer getNextRow() {
        return nextRow == null ? null : nextRow.duplicate().asReadOnlyBuffer();
    }

    public void setNextRow(byte[] nextRow) {
        if (nextRow != null) {
            this.nextRow = ByteBuffer.wrap(nextRow).asReadOnlyBuffer();
        } else {
            this.nextRow = null;
        }
    }

    public int getPageSize() {
        return pageSize;
    }

    @Override
    public String toString() {
        return "HBasePaginatedQuery{" +
                "pageSize=" + pageSize +
                ", nextRow=" + toStringBinary(nextRow) +
                '}';
    }
}
