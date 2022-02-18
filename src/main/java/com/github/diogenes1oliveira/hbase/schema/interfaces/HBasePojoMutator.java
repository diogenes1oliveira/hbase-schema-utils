package com.github.diogenes1oliveira.hbase.schema.interfaces;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.singleton;

/**
 * Interface to effectively build and execute the Mutations corresponding to a POJO object
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBasePojoMutator<T> {
    /**
     * Effects into HBase the mutations corresponding to the input POJOs
     *
     * @param pojos              POJO objects to insert into HBase
     * @param mutationsGenerator object to map the POJOs into Mutations
     * @return list of inserted row keys
     * @throws IOException HBase error while mutating
     */
    List<byte[]> mutate(Collection<? extends T> pojos, HBaseMutationsGenerator<T> mutationsGenerator) throws IOException;

    /**
     * Effects into HBase the mutations corresponding to a single POJO
     * <p>
     * The default implementation just forwards to {@link #mutate(Collection, HBaseMutationsGenerator)}
     *
     * @param pojo               POJO object to insert into HBase
     * @param mutationsGenerator object to map the POJOs into Mutations
     * @return list of inserted row keys
     * @throws IOException HBase error while mutating
     */
    default byte[] mutate(T pojo, HBaseMutationsGenerator<T> mutationsGenerator) throws IOException {
        List<byte[]> rowKeys = mutate(singleton(pojo), mutationsGenerator);
        return rowKeys.get(0);
    }
}
