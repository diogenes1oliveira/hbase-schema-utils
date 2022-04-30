package hbase.schema.api.utils;

import hbase.schema.api.interfaces.HBaseReadSchema;
import hbase.schema.api.interfaces.HBaseReadSchemaWrapper;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

public class HBaseScanLimit<Q, R> extends HBaseReadSchemaWrapper<Q, R> {
    private final Function<Q, Integer> limitGetter;

    public HBaseScanLimit(HBaseReadSchema<Q, R> schema, Function<Q, Integer> limitGetter) {
        super(schema);
        this.limitGetter = limitGetter;
    }

    public HBaseScanLimit(HBaseReadSchema<Q, R> schema, int limit) {
        this(schema, q -> limit);
    }

    @Override
    public List<Scan> toScans(Q query) {
        List<Scan> scans = super.toScans(query);

        Integer limit = limitGetter.apply(query);
        if (limit == null || limit < 0) {
            return scans;
        }

        return scans.stream().map(s -> s.setLimit(limit)).collect(toList());
    }
}
