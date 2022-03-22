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
     * @param objects source objects
     * @throws IOException           failed to execute the mutations
     * @throws IllegalStateException interrupted while mutating
     */
    void mutate(List<T> objects) throws IOException;

}
