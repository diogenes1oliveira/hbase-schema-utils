package hbase.schema.api.interfaces;

import org.apache.hadoop.hbase.client.Get;

/**
 * Interface to generate Get queries corresponding to a POJO query
 *
 * @param <T> POJO type
 */
@FunctionalInterface
public interface HBaseGetGenerator<T> {
    /**
     * Builds a Get request based on the data from a POJO object
     *
     * @param query      POJO object to act as query source data
     * @param readSchema object to generate the Get data from the POJO
     * @return built Get request
     */
    Get toGet(T query, HBaseReadSchema<T> readSchema);

}
