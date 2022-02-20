package hbase.schema.api.interfaces;

import hbase.schema.api.interfaces.converters.HBaseBytesExtractor;
import hbase.schema.api.interfaces.converters.HBaseDeltasMapper;
import hbase.schema.api.interfaces.converters.HBaseLongExtractor;
import hbase.schema.api.interfaces.converters.HBaseValuesMapper;

import java.util.List;

/**
 * Interface to generate HBase Put and Increment data from the fields of a POJO object
 *
 * @param <T> POJO type
 */
public interface HBaseWriteSchema<T> {
    /**
     * Object to generate the row key
     *
     * @return row key parser
     */
    HBaseBytesExtractor<T> getRowKeyGenerator();

    /**
     * Object to generate a millis-timestamp
     *
     * @return timestamp generator
     */
    HBaseLongExtractor<T> getTimestampGenerator();

    /**
     * Object to generate a map of byte[] values
     *
     * @return bytes value generator
     */
    List<HBaseValuesMapper<T>> getPutGenerators();

    /**
     * Object to generate a map of increment deltas
     *
     * @return long increment generator
     */
    List<HBaseDeltasMapper<T>> getIncrementGenerators();
}
