package hbase.schema.connector.interfaces;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.singleton;

/**
 * Interface to effectively build and execute the Mutations corresponding to POJO objects
 */
@FunctionalInterface
public interface HBasePojoMutationBuilder {
    /**
     * Effects into HBase the mutations corresponding to the input POJOs
     *
     * @param pojos POJO objects to insert into HBase
     * @param <T>   POJO type
     * @return list of inserted row keys
     * @throws IOException HBase error while mutating
     */
    <T> List<byte[]> mutate(Collection<? extends T> pojos) throws IOException;

    /**
     * Effects into HBase the mutations corresponding to a single POJO
     * <p>
     * The default implementation just forwards to {@link #mutate(Collection)}
     *
     * @param pojo POJO object to insert into HBase
     * @param <T>  POJO type
     * @return list of inserted row keys
     * @throws IOException HBase error while mutating
     */
    default <T> byte[] mutate(T pojo) throws IOException {
        List<byte[]> rowKeys = mutate(singleton(pojo));
        return rowKeys.get(0);
    }
}
