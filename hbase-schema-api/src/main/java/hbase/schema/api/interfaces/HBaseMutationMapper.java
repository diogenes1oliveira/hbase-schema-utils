package hbase.schema.api.interfaces;

import hbase.schema.api.models.HBaseDeltaCell;
import hbase.schema.api.models.HBaseValueCell;
import org.jetbrains.annotations.Nullable;

import java.util.List;

import static java.util.Collections.emptyList;

public interface HBaseMutationMapper<T> {
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
}
