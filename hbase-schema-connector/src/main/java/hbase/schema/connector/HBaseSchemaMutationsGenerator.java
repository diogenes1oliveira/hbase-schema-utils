package hbase.schema.connector;

import hbase.schema.api.interfaces.HBaseMutationSchema;
import hbase.schema.connector.interfaces.HBaseMutationsGenerator;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;

import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

/**
 * Builds HBase Mutations from Java objects according to a schema, that act as source data
 *
 * @param <T> object type
 */
public class HBaseSchemaMutationsGenerator<T> implements HBaseMutationsGenerator<T> {
    private final byte[] family;
    private final HBaseMutationSchema<T> mutationSchema;

    /**
     * @param family         target column family
     * @param mutationSchema mutation schema object
     */
    public HBaseSchemaMutationsGenerator(byte[] family, HBaseMutationSchema<T> mutationSchema) {
        this.family = family;
        this.mutationSchema = mutationSchema;
    }

    /**
     * Creates a list of mutations for the source object
     *
     * @param object source object
     * @return list of mutations corresponding to the source object
     */
    @Override
    public List<Mutation> toMutations(T object) {
        List<Mutation> mutations = new ArrayList<>();

        mutations.add(toPut(object));
        mutations.add(toIncrement(object));

        mutations.removeIf(Objects::isNull);
        return mutations;
    }

    /**
     * Creates a Put for the source object
     *
     * @param object source object
     * @return Put or null if no {@code byte[]} value was generated for the source object
     */
    @Nullable
    public Put toPut(T object) {
        byte[] rowKey = mutationSchema.buildRowKey(object);
        Long rowTimestamp = mutationSchema.buildTimestamp(object);
        if (rowKey == null || rowTimestamp == null) {
            return null;
        }

        NavigableMap<byte[], byte[]> putValues = mutationSchema.buildPutValues(object);
        if (putValues.isEmpty()) {
            return null;
        }
        Put put = new Put(rowKey, rowTimestamp);

        for (Map.Entry<byte[], byte[]> entry : putValues.entrySet()) {
            byte[] qualifier = entry.getKey();
            byte[] value = entry.getValue();
            long timestamp = firstNonNull(mutationSchema.buildTimestamp(object, qualifier), rowTimestamp);
            put = put.addColumn(family, qualifier, timestamp, value);
        }

        return put;
    }

    /**
     * Creates a Increment for the source object
     *
     * @param object source object
     * @return Increment or null if no {@code Long} value was generated for the source object
     */
    @Nullable
    public Increment toIncrement(T object) {
        byte[] rowKey = mutationSchema.buildRowKey(object);
        Long rowTimestamp = mutationSchema.buildTimestamp(object);
        if (rowKey == null || rowTimestamp == null) {
            return null;
        }

        NavigableMap<byte[], Long> incrementValues = mutationSchema.buildIncrementValues(object);
        if (incrementValues.isEmpty()) {
            return null;
        }
        Increment increment = new Increment(rowKey).setTimestamp(rowTimestamp);

        for (Map.Entry<byte[], Long> entry : incrementValues.entrySet()) {
            byte[] qualifier = entry.getKey();
            Long value = entry.getValue();
            increment = increment.addColumn(family, qualifier, value);
        }

        return increment;
    }
}
