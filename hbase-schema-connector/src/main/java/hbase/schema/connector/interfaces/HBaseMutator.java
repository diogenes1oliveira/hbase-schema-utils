package hbase.schema.connector.interfaces;

import java.io.IOException;
import java.util.List;

/**
 * Interface to insert objects into HBase
 *
 * @param <T> source object type
 */
@FunctionalInterface
public interface HBaseMutator<T> {
    /**
     * Builds and executes the mutations corresponding to the source objects
     *
     * @param tableName name of the table to insert data in
     * @param objects   source objects
     * @throws IOException           failed to execute the mutations
     * @throws IllegalStateException interrupted while mutating
     */
    void mutate(String tableName, List<T> objects) throws IOException;

}
