package hbase.schema.connector.services;

import hbase.base.exceptions.UncheckedInterruptionException;
import hbase.connector.services.HBaseConnector;
import hbase.schema.api.interfaces.HBaseMutationSchema;
import hbase.schema.api.interfaces.HBaseSchema;
import hbase.schema.connector.interfaces.HBaseMutator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;

import static org.apache.commons.lang3.ObjectUtils.firstNonNull;

/**
 * Interface to insert objects into HBase according to a schema
 *
 * @param <T> source object type
 */
public class HBaseSchemaMutator<T> implements HBaseMutator<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSchemaMutator.class);

    private final byte[] family;
    private final HBaseMutationSchema<T> mutationSchema;
    private final HBaseConnector connector;

    /**
     * @param family    column family
     * @param schema    object schema
     * @param connector connector object
     */
    public HBaseSchemaMutator(byte[] family, HBaseSchema<T, ?, ?> schema, HBaseConnector connector) {
        this.family = family;
        this.mutationSchema = schema.mutationSchema();
        this.connector = connector;
    }

    /**
     * Builds and executes the mutations corresponding to the source objects
     *
     * @param tableName name of the table to insert data in
     * @param objects   source objects
     * @throws IOException                    failed to execute the mutations
     * @throws UncheckedInterruptionException interrupted while mutating
     */
    @Override
    public void mutate(TableName tableName, List<T> objects) throws IOException {
        List<Mutation> mutations = new ArrayList<>();

        for (T object : objects) {
            mutations.addAll(toMutations(object));
        }

        if (mutations.isEmpty()) {
            return;
        }

        try (Connection connection = connector.context();
             Table table = connection.getTable(tableName)) {
            table.batch(mutations, new Object[mutations.size()]);
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while mutating", e);
            Thread.currentThread().interrupt();
            throw new UncheckedInterruptionException("Interrupted while mutating", e);
        }
    }

    /**
     * Creates a list of mutations for the source object
     *
     * @param object source object
     * @return list of mutations corresponding to the source object
     */
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
