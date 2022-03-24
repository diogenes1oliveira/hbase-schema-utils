package hbase.schema.api.interfaces;

import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Set;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;
import static java.util.Collections.unmodifiableSet;

public interface HBaseQueryMapper<T> {
    Set<byte[]> EMPTY = unmodifiableSet(asBytesTreeSet());

    @NotNull
    default Set<byte[]> prefixes() {
        return EMPTY;
    }

    @NotNull
    default Set<byte[]> qualifiers() {
        return EMPTY;
    }

    byte[] toRowKey(T query);

    List<Pair<byte[], byte[]>> toSearchRanges(T query);
}
