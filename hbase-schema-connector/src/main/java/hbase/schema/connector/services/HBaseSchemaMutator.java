package hbase.schema.connector.services;

import hbase.connector.HBaseConnector;
import hbase.schema.api.interfaces.HBaseSchema;
import hbase.schema.connector.interfaces.HBaseMutationsGenerator;
import hbase.schema.connector.interfaces.HBaseMutator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Interface to insert objects into HBase according to a schema
 *
 * @param <T> source object type
 */
public class HBaseSchemaMutator<T> implements HBaseMutator<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseSchemaMutator.class);

    private final HBaseMutationsGenerator<T> mutationsGenerator;
    private final HBaseConnector connector;

    /**
     * @param family    column family
     * @param schema    object schema
     * @param connector connector object
     */
    public HBaseSchemaMutator(byte[] family, HBaseSchema<T, ?> schema, HBaseConnector connector) {
        this.mutationsGenerator = new HBaseSchemaMutationsGenerator<>(family, schema.mutationSchema());
        this.connector = connector;
    }

    /**
     * Builds and executes the mutations corresponding to the source objects
     *
     * @param tableName name of the table to insert data in
     * @param objects   source objects
     * @throws IOException           failed to execute the mutations
     * @throws IllegalStateException interrupted while mutating
     */
    @Override
    public void mutate(TableName tableName, List<T> objects) throws IOException {
        List<Mutation> mutations = mutationsGenerator.toMutations(objects);

        try (Connection connection = connector.connect();
             Table table = connection.getTable(tableName)) {
            table.batch(mutations, new Object[mutations.size()]);
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while mutating", e);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while mutating", e);
        }
    }
}
