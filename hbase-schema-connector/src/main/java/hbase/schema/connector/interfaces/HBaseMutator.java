package hbase.schema.connector.interfaces;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;

import java.io.IOException;
import java.util.List;

/**
 * Interface to insert objects into HBase
 *
 * @param <T> source object type
 */
public interface HBaseMutator<T> {
    /**
     * Builds and executes the mutations corresponding to the source objects
     *
     * @param tableName name of the table to insert data in
     * @param objects   source objects
     * @throws IOException           failed to execute the mutations
     * @throws IllegalStateException interrupted while mutating
     */
    void mutate(TableName tableName, List<T> objects) throws IOException;

    /**
     * Creates a list of mutations for the source object
     *
     * @param object source object
     * @return list of mutations corresponding to the source object
     */
    List<Mutation> toMutations(T object);

}
