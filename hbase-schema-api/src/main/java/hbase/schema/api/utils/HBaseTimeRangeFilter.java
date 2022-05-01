package hbase.schema.api.utils;

import hbase.schema.api.interfaces.HBaseLongMapper;
import hbase.schema.api.interfaces.HBaseQueryCustomizer;
import hbase.schema.api.interfaces.conversion.LongConverter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static hbase.schema.api.utils.HBaseSchemaUtils.chain;
import static java.util.stream.Collectors.toList;

public class HBaseTimeRangeFilter<Q> implements HBaseQueryCustomizer<Q> {
    private final HBaseLongMapper<Q> startGetter;
    private final HBaseLongMapper<Q> endGetter;

    private HBaseTimeRangeFilter(HBaseLongMapper<Q> startGetter, HBaseLongMapper<Q> endGetter) {
        this.startGetter = startGetter;
        this.endGetter = endGetter;
    }

    @Override
    public List<Scan> customize(Q query, List<Scan> scans) {
        Pair<Long, Long> range = toRange(query);
        if (range == null) {
            return scans;
        }
        return scans.stream()
                    .map(scan -> {
                        try {
                            return scan.setTimeRange(range.getLeft(), range.getRight());
                        } catch (IOException e) {
                            throw new IllegalArgumentException("Invalid time range", e);
                        }
                    })
                    .collect(toList());
    }

    @Override
    public Get customize(Q query, Get get) {
        Pair<Long, Long> range = toRange(query);
        if (range == null) {
            return get;
        }
        try {
            return get.setTimeRange(range.getLeft(), range.getRight());
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid time range", e);
        }
    }

    private Pair<Long, Long> toRange(Q query) {
        Long start = startGetter.toLong(query);
        Long end = endGetter.toLong(query);
        if (start == null || end == null || start <= 0 || end <= 0) {
            return null;
        } else {
            return Pair.of(start, end);
        }
    }

    public static <Q> HBaseTimeRangeFilter<Q> hBaseTimeRangeBuilder(HBaseLongMapper<Q> startGetter,
                                                                    HBaseLongMapper<Q> endGetter) {
        return new HBaseTimeRangeFilter<>(startGetter, endGetter);
    }

    public static <Q, S, E> HBaseTimeRangeFilter<Q> hBaseTimeRangeBuilder(Function<Q, S> startGetter,
                                                                          Function<Q, E> endGetter,
                                                                          Function<S, Long> startConverter,
                                                                          Function<E, Long> endConverter) {
        return hBaseTimeRangeBuilder(chain(startGetter, startConverter)::apply, chain(endGetter, endConverter)::apply);
    }

    public static <Q, S, E> HBaseTimeRangeFilter<Q> hBaseTimeRangeBuilder(Function<Q, S> startGetter,
                                                                          Function<Q, E> endGetter,
                                                                          LongConverter<S> startConverter,
                                                                          LongConverter<E> endConverter) {
        return hBaseTimeRangeBuilder(startGetter, endGetter, startConverter::toLong, endConverter::toLong);
    }

    public static <Q, T> HBaseTimeRangeFilter<Q> hBaseTimeRangeBuilder(Function<Q, T> startGetter,
                                                                       Function<Q, T> endGetter,
                                                                       Function<T, Long> converter) {
        return hBaseTimeRangeBuilder(startGetter, endGetter, converter, converter);
    }

    public static <Q, T> HBaseTimeRangeFilter<Q> hBaseTimeRangeBuilder(Function<Q, T> startGetter,
                                                                       Function<Q, T> endGetter,
                                                                       LongConverter<T> converter) {
        return hBaseTimeRangeBuilder(startGetter, endGetter, converter, converter);
    }

}
