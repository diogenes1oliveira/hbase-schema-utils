package hbase.schema.connector.interfaces;

import org.apache.hadoop.hbase.client.Mutation;

import java.util.List;

public interface HBaseMutationBuilder<T> {
    List<Mutation> toMutations(byte[] family, T obj);
}
