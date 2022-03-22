package hbase.schema.connector.services;

import hbase.base.exceptions.UncheckedInterruptionException;
import hbase.connector.services.HBaseConnector;
import hbase.schema.api.interfaces.HBaseSchema;
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

    private final TableName tableName;
    private final byte[] family;
    private final HBaseMutationBuilder<T> mutationBuilder;
    private final HBaseConnector connector;

    /**
     * @param schema    schema
     * @param connector connector object
     */
    public HBaseSchemaMutator(String tableName,
                              byte[] family,
                              HBaseSchema<T, ?> schema,
                              HBaseConnector connector) {
        this.tableName = TableName.valueOf(tableName);
        this.family = family;
        this.mutationBuilder = new HBaseCellsMutationBuilder<>(schema.mutationMapper());
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
    public void mutate(List<T> objects) throws IOException {
        List<Mutation> mutations = new ArrayList<>();

        for (T object : objects) {
            mutations.addAll(mutationBuilder.toMutations(family, object));
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
        }
    }

}
