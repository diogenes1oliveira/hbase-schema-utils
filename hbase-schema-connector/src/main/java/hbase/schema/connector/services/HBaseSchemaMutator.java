package hbase.schema.connector.services;

import hbase.base.exceptions.UncheckedInterruptionException;
import hbase.connector.services.HBaseConnector;
import hbase.schema.connector.interfaces.HBaseMutationBuilder;
import hbase.schema.connector.interfaces.HBaseMutator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Interface to insert objects into HBase according to a schema
 *
 * @param <T> source object type
 */
public class HBaseSchemaMutator<T> implements HBaseMutator<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSchemaMutator.class);

    private final byte[] family;
    private final HBaseMutationBuilder<T> mutationBuilder;
    private final HBaseConnector connector;

    /**
     * @param family          column family
     * @param mutationBuilder object to build mutations from Java objects
     * @param connector       connector object
     */
    public HBaseSchemaMutator(byte[] family, HBaseMutationBuilder<T> mutationBuilder, HBaseConnector connector) {
        this.family = family;
        this.mutationBuilder = mutationBuilder;
        this.connector = connector;
    }

    /**
     * Builds and executes the mutations corresponding to the source objects
     *
     * @param objects source objects
     * @throws IOException                    failed to execute the mutations
     * @throws UncheckedInterruptionException interrupted while mutating
     */
    @Override
    public void mutate(String tableName, List<T> objects) throws IOException {
        List<Mutation> mutations = new ArrayList<>();

        for (T object : objects) {
            mutations.addAll(mutationBuilder.toMutations(family, object));
        }

        if (mutations.isEmpty()) {
            return;
        }

        try (Connection connection = connector.context();
             Table table = connection.getTable(TableName.valueOf(tableName))) {
            table.batch(mutations, new Object[mutations.size()]);
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while mutating", e);
            Thread.currentThread().interrupt();
        }
    }

}
