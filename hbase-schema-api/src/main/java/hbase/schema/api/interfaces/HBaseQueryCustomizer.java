package hbase.schema.api.interfaces;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

public interface HBaseQueryCustomizer<Q> {
    default Scan customize(Q query, Scan scan) {
        return scan;
    }

    default Get customize(Q query, Get get) {
        return get;
    }
}
