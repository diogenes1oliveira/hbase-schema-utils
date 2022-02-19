package com.github.diogenes1oliveira.hbase.schema.api.interfaces;

import org.apache.hadoop.hbase.client.Mutation;

import java.util.List;

/**
 * Interface to generate the Mutations corresponding to a POJO object
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseMutationsGenerator<T> {
    /**
     * Builds the list of mutations to insert data into HBase
     *
     * @param pojo        POJO object
     * @param writeSchema schema to generate the values to insert into HBase
     * @return list of mutations
     */
    List<Mutation> toMutations(T pojo, HBaseWriteSchema<T> writeSchema);
}
