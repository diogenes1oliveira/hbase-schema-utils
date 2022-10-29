package hbase.schema.api.schema;

import hbase.schema.api.interfaces.BytesMapper;
import hbase.schema.api.interfaces.HBaseQuerier;
import hbase.schema.api.interfaces.LongMapper;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

public class HBaseQuerierBuilder {
    private final byte[] columnFamily;
    private final byte[] separator;
    private final Map<String, List<byte[]>> bucketsMap = new HashMap<>();

    public HBaseQuerierBuilder(String columnFamily, String separator) {
        this.columnFamily = columnFamily.getBytes(StandardCharsets.UTF_8);
        this.separator = separator.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Sets a row key prefix
     *
     * @param name         fragment name
     * @param prefixMapper maps the fragment value to a {@code byte[]} prefix
     * @return this builder
     */
    public HBaseQuerierBuilder fragment(String name, BytesMapper<String> prefixMapper) {
        return this;
    }

    /**
     * Sets a row key range [start, end)
     *
     * @param name        fragment name
     * @param startMapper maps the fragment value to a {@code byte[]} range start
     * @param stopMapper  maps the fragment value to a {@code byte[]} range stop
     * @return this builder
     */
    public HBaseQuerierBuilder fragment(String name, BytesMapper<String> startMapper, BytesMapper<String> stopMapper) {
        return this;
    }

    /**
     * Sets the scan buckets for a fragment
     *
     * @param name    fragment name
     * @param buckets list of bucket values for scans
     * @return this builder
     */
    public HBaseQuerierBuilder buckets(String name, List<String> buckets) {
        List<byte[]> bucketBytes = buckets.stream()
                                          .map(s -> s.getBytes(StandardCharsets.UTF_8))
                                          .collect(toCollection(ArrayList::new));
        bucketsMap.put(name, bucketBytes);
        return this;
    }

    /**
     * Sets the scan buckets for a fragment
     *
     * @param name         fragment name
     * @param numBuckets   number of buckets to generate
     * @param bucketMapper maps a number in {@code [0, numBuckets)} to a bucket value
     * @return this builder
     */
    public HBaseQuerierBuilder buckets(String name, int numBuckets, IntFunction<String> bucketMapper) {
        List<String> buckets = IntStream.range(0, numBuckets).mapToObj(bucketMapper).collect(toList());
        return buckets(name, buckets);
    }

    /**
     * Adds a constant value to the row/search key
     *
     * @param value value to add
     * @return this builder
     */
    public HBaseQuerierBuilder constant(String value) {
        return this;
    }

    /**
     * Sets the time range
     *
     * @param minName         param for the time range minimum
     * @param maxName         param for the time range maximum
     * @param timestampMapper maps the value to a timestamp
     * @return this builder
     */
    public HBaseQuerierBuilder timeRange(String minName, String maxName, LongMapper<String> timestampMapper) {
        return this;
    }

    public HBaseQuerier build() {
        return new HBaseQuerier() {
            @Override
            public Get toGet(Map<String, String> params) {
                return null;
            }

            @Override
            public List<Scan> toScans(Map<String, String> params) {
                Scan scan = new Scan();
                return null;
            }
        };
    }
}
