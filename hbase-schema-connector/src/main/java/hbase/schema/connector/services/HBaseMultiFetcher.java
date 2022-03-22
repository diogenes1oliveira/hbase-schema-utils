package hbase.schema.connector.services;

import hbase.schema.connector.interfaces.HBaseFetcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toCollection;

public class HBaseMultiFetcher<Q, R> implements HBaseFetcher<Q, R> {
    private final Comparator<R> resultComparator;
    private final List<HBaseFetcher<Q, R>> fetchers;

    public HBaseMultiFetcher(Comparator<R> resultComparator, Collection<HBaseFetcher<Q, R>> fetchers) {
        this.resultComparator = resultComparator;
        this.fetchers = new ArrayList<>(fetchers);
    }

    @Override
    public List<R> get(List<? extends Q> queries) throws IOException {
        if (fetchers.size() == 1) {
            return fetchers.get(0).get(queries);
        }
        try {
            Stream<R> stream = ioParallelMap(fetchers, f -> f.get(queries));
            return uniqueCollect(stream);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    @Override
    public List<R> scan(List<? extends Q> queries) throws IOException {
        if (fetchers.size() == 1) {
            return fetchers.get(0).scan(queries);
        }
        try {
            Stream<R> stream = ioParallelMap(fetchers, f -> f.scan(queries));
            return uniqueCollect(stream);
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private <T, U> Stream<U> ioParallelMap(Collection<T> input, IOFunction<T, ? extends Collection<U>> mapper) {
        return input.parallelStream()
                    .map(t -> {
                        try {
                            return mapper.apply(t);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .flatMap(Collection::stream);
    }

    private List<R> uniqueCollect(Stream<R> stream) {
        TreeSet<R> result = stream.collect(toCollection(() -> new TreeSet<>(resultComparator)));
        return new ArrayList<>(result);
    }

    @FunctionalInterface
    private interface IOFunction<T, U> {
        U apply(T t) throws IOException;
    }
}
