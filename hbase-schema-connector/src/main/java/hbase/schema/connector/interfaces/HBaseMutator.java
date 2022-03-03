package hbase.schema.connector.interfaces;

import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * Interface to insert objects into HBase
 *
 * @param <T> source object type
 */
@FunctionalInterface
public interface HBaseMutator<T> {
    /**
     * Builds and executes the mutations corresponding to the source object
     * <p>
     * The default implementation just delegates to {@link #mutate(TableName, List)}
     *
     * @param tableName name of the table to insert data in
     * @param object    source object
     * @throws IOException           failed to execute the mutations
     * @throws IllegalStateException interrupted while mutating
     */
    default void mutate(TableName tableName, T object) throws IOException {
        mutate(tableName, singletonList(object));
    }

    /**
     * Builds and executes the mutations corresponding to the source objects
     *
     * @param tableName name of the table to insert data in
     * @param objects   source objects
     * @throws IOException           failed to execute the mutations
     * @throws IllegalStateException interrupted while mutating
     */
    void mutate(TableName tableName, List<T> objects) throws IOException;

}
