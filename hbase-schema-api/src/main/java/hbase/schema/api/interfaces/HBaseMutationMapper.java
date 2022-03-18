package hbase.schema.api.interfaces;

import hbase.schema.api.models.HBaseDeltaCell;
import hbase.schema.api.models.HBaseValueCell;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;
import static java.util.Collections.emptyList;

public interface HBaseMutationMapper<T> {
    Set<byte[]> EMPTY = Collections.unmodifiableSet(asBytesTreeSet());

    byte @Nullable [] toRowKey(T obj);

    @Nullable
    default Long toTimestamp(T obj) {
        return null;
    }

    default List<HBaseDeltaCell> toDeltas(T obj) {
        return emptyList();
    }

    default List<HBaseValueCell> toValues(T obj) {
        return emptyList();
    }


    @NotNull
    default Set<byte[]> prefixes() {
        return EMPTY;
    }

    @NotNull
    default Set<byte[]> qualifiers() {
        return EMPTY;
    }
}
