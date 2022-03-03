package hbase.schema.connector.interfaces;

import org.apache.hadoop.hbase.client.Mutation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Builds HBase Mutations from Java objects, that act as source data
 *
 * @param <T> object type
 */
@FunctionalInterface
public interface HBaseMutationsGenerator<T> {
    /**
     * Creates a list of mutations for the source object
     *
     * @param object source object
     * @return list of mutations corresponding to the source object
     */
    List<Mutation> toMutations(T object);

    /**
     * Creates a list of mutations for the source objects
     * <p>
     * The default implementation just concatenates the results of {@link #toMutations(T)}
     *
     * @param objects collection of source objects
     * @return list of mutations corresponding to the source objects
     */
    default List<Mutation> toMutations(Collection<? extends T> objects) {
        List<Mutation> mutations = new ArrayList<>();

        for (T object : objects) {
            mutations.addAll(toMutations(object));
        }

        return mutations;
    }
}
