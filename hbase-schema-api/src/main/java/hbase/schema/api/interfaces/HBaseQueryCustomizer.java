package hbase.schema.api.interfaces;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;
import java.util.function.BiFunction;

public interface HBaseQueryCustomizer<Q> {
    default List<Scan> customize(Q query, List<Scan> scans) {
        return scans;
    }

    default Get customize(Q query, Get get) {
        return get;
    }

    static <Q> HBaseQueryCustomizer<Q> getCustomizer(BiFunction<Q, Get, Get> customizer) {
        return new HBaseQueryCustomizer<Q>() {
            @Override
            public Get customize(Q query, Get get) {
                return customizer.apply(query, get);
            }
        };
    }
}
