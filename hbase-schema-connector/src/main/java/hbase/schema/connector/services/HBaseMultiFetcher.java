package hbase.schema.connector.services;

import hbase.schema.connector.interfaces.HBaseFetcher;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static java.util.Arrays.asList;

public class HBaseMultiFetcher<Q, R> {
    private final Function<R, ?> idGetter;
    private final List<HBaseFetcher<Q, R>> fetchers;

    @SafeVarargs
    public HBaseMultiFetcher(Function<R, ?> idGetter, HBaseFetcher<Q, R>... fetchers) {
        this(idGetter, asList(fetchers));
    }

    public HBaseMultiFetcher(Function<R, ?> idGetter, List<HBaseFetcher<Q, R>> fetchers) {
        this.idGetter = idGetter;
        this.fetchers = fetchers;
    }

    public List<R> get(Q query) throws IOException {
        return null;
    }

    public List<R> scan(List<? extends Q> queries) throws IOException {
        return null;
    }

}
