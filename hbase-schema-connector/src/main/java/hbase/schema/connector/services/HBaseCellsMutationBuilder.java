package hbase.schema.connector.services;


import hbase.schema.api.interfaces.HBaseMutationMapper;
import hbase.schema.api.models.HBaseDeltaCell;
import hbase.schema.api.models.HBaseValueCell;
import hbase.schema.connector.interfaces.HBaseMutationBuilder;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

public class HBaseCellsMutationBuilder<T> implements HBaseMutationBuilder<T> {
    private final HBaseMutationMapper<T> mutationMapper;

    public HBaseCellsMutationBuilder(HBaseMutationMapper<T> mutationMapper) {
        this.mutationMapper = mutationMapper;
    }

    @Override
    public List<Mutation> toMutations(byte[] family, T obj) {
        List<Mutation> mutations = new ArrayList<>();

        Put put = toPut(family, obj);
        Increment increment = toIncrement(family, obj);

        if (put != null) {
            mutations.add(put);
        }

        if (increment != null) {
            mutations.add(increment);
        }

        return mutations;
    }

    /**
     * Creates a Put for the source object
     *
     * @param obj source object
     * @return Put or null if no {@code byte[]} value was generated for the source object
     */
    @Nullable
    private Put toPut(byte[] family, T obj) {
        byte[] rowKey = mutationMapper.toRowKey(obj);
        if (rowKey == null) {
            return null;
        }
        Put put;
        Long rowTimestamp = mutationMapper.toTimestamp(obj);
        if (rowTimestamp == null) {
            put = new Put(rowKey);
        } else {
            put = new Put(rowKey, rowTimestamp);
        }
        boolean hasValue = false;

        for (HBaseValueCell cell : mutationMapper.toValues(obj)) {
            byte[] qualifier = cell.getQualifier();
            byte[] value = cell.getValue();
            if (value == null) {
                continue;
            }
            hasValue = true;
            Long timestamp = cell.getTimestamp();
            if (timestamp == null) {
                timestamp = rowTimestamp;
            }
            if (timestamp == null) {
                put = put.addColumn(family, qualifier, value);
            } else {
                put = put.addColumn(family, qualifier, timestamp, value);
            }
        }

        return hasValue ? put : null;
    }

    /**
     * Creates an Increment for the source object
     *
     * @param obj source object
     * @return Increment or null if no {@code Long} value was generated for the source object
     */
    @Nullable
    private Increment toIncrement(byte[] family, T obj) {
        byte[] rowKey = mutationMapper.toRowKey(obj);
        if (rowKey == null) {
            return null;
        }
        Increment increment = new Increment(rowKey);
        boolean hasValue = false;

        for (HBaseDeltaCell cell : mutationMapper.toDeltas(obj)) {
            byte[] qualifier = cell.getQualifier();
            Long value = cell.getValue();
            if (value == null || value == 0L) {
                continue;
            }
            hasValue = true;
            increment.addColumn(family, qualifier, value);
        }

        return hasValue ? increment : null;
    }
}
