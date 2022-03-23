package hbase.schema.connector.services;

import hbase.schema.connector.interfaces.HBaseFetcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class HBaseMultiFetcher<Q, R> implements HBaseFetcher<Q, R> {
    private final List<HBaseFetcher<Q, R>> fetchers;

    public HBaseMultiFetcher(Collection<HBaseFetcher<Q, R>> fetchers) {
        this.fetchers = new ArrayList<>(fetchers);
    }

    @Override
    public List<R> get(List<? extends Q> queries) throws IOException {
        if (fetchers.size() == 1) {
            return fetchers.get(0).get(queries);
        }
        try {
            return ioParallelMap(fetchers, f -> f.get(queries));
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
            return ioParallelMap(fetchers, f -> f.scan(queries));
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private <T, U> List<U> ioParallelMap(Collection<T> input, IOFunction<T, ? extends Collection<U>> mapper) {
        return input.parallelStream()
                    .map(t -> {
                        try {
                            return mapper.apply(t);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .flatMap(Collection::stream)
                    .collect(toList());
    }

    @FunctionalInterface
    private interface IOFunction<T, U> {
        U apply(T t) throws IOException;
    }
}
