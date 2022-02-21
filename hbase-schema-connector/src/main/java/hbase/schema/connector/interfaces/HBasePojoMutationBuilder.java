package hbase.schema.connector.interfaces;

import hbase.schema.api.interfaces.HBaseWriteSchema;
import hbase.schema.api.interfaces.converters.HBaseCellsMapper;
import hbase.schema.api.interfaces.converters.HBaseLongsMapper;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

/**
 * Interface to effectively build and execute the Mutations corresponding to POJO objects
 */
@FunctionalInterface
public interface HBasePojoMutationBuilder<T> {
    List<Mutation> toMutations(T obj);

    default List<Mutation> toMutations(Collection<? extends T> objs) {
        List<Mutation> allMutations = new ArrayList<>();

        for (T obj : objs) {
            List<Mutation> objMutations = toMutations(obj);
            allMutations.addAll(objMutations);
        }

        return allMutations;
    }

    default List<Mutation> toMutations(T obj, byte[] family, HBaseWriteSchema<T> writeSchema) {
        byte[] rowKey = writeSchema.getRowKeyGenerator().getBytes(obj);
        Long nullableTimestamp = writeSchema.getTimestampGenerator().getLong(obj);
        if (rowKey == null || nullableTimestamp == null) {
            return emptyList();
        }
        long timestamp = nullableTimestamp;

        Put put = new Put(rowKey, timestamp);
        boolean putHasData = false;
        Increment increment = new Increment(rowKey);
        boolean incrementHasData = false;

        for (HBaseCellsMapper<T> mapper : writeSchema.getPutGenerators()) {
            Map<byte[], byte[]> cellsValues = mapper.getBytes(obj);
            for (Map.Entry<byte[], byte[]> entry : cellsValues.entrySet()) {
                byte[] qualifier = entry.getKey();
                byte[] value = entry.getValue();
                if (value != null) {
                    put.addColumn(family, qualifier, timestamp, value);
                    putHasData = true;
                }
            }
        }

        for (HBaseLongsMapper<T> mapper : writeSchema.getIncrementGenerators()) {
            for (Map.Entry<byte[], Long> entry : mapper.getLongs(obj).entrySet()) {
                byte[] qualifier = entry.getKey();
                Long value = entry.getValue();
                if (value != null) {
                    increment.addColumn(family, qualifier, value);
                    incrementHasData = true;
                }
            }
        }


        List<Mutation> mutations = new ArrayList<>();
        if (putHasData) {
            mutations.add(put);
        }

        if (incrementHasData) {
            mutations.add(increment);
        }

        return mutations;
    }

}
