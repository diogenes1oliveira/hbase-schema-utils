package hbase.schema.api.interfaces;

import hbase.schema.api.models.HBaseDeltaCell;
import hbase.schema.api.models.HBaseValueCell;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;
import static java.util.Collections.emptyList;

public interface HBaseCellsMapper<T> {
    Set<byte[]> EMPTY = Collections.unmodifiableSet(asBytesTreeSet());

    default List<HBaseDeltaCell> toDeltas(T object) {
        return emptyList();
    }

    default List<HBaseValueCell> toValues(T object) {
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
