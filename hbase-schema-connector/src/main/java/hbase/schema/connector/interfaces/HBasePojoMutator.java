package hbase.schema.connector.interfaces;

import hbase.schema.api.interfaces.HBaseMutationsGenerator;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.singleton;

/**
 * Interface to effectively build and execute the Mutations corresponding to POJO objects
 */
@FunctionalInterface
public interface HBasePojoMutator {
    /**
     * Effects into HBase the mutations corresponding to the input POJOs
     *
     * @param pojos              POJO objects to insert into HBase
     * @param mutationsGenerator object to map the POJOs into Mutations
     * @param <T>                POJO type
     * @return list of inserted row keys
     * @throws IOException HBase error while mutating
     */
    <T> List<byte[]> mutate(Collection<? extends T> pojos, HBaseMutationsGenerator<T> mutationsGenerator) throws IOException;

    /**
     * Effects into HBase the mutations corresponding to a single POJO
     * <p>
     * The default implementation just forwards to {@link #mutate(Collection, HBaseMutationsGenerator)}
     *
     * @param pojo               POJO object to insert into HBase
     * @param mutationsGenerator object to map the POJOs into Mutations
     * @param <T>                POJO type
     * @return list of inserted row keys
     * @throws IOException HBase error while mutating
     */
    default <T> byte[] mutate(T pojo, HBaseMutationsGenerator<T> mutationsGenerator) throws IOException {
        List<byte[]> rowKeys = mutate(singleton(pojo), mutationsGenerator);
        return rowKeys.get(0);
    }
}
