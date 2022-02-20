package hbase.schema.api.interfaces;

import hbase.schema.api.interfaces.converters.HBaseBytesGetter;
import hbase.schema.api.interfaces.converters.HBaseLongsMapper;
import hbase.schema.api.interfaces.converters.HBaseLongGetter;
import hbase.schema.api.interfaces.converters.HBaseCellsMapper;

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
    HBaseBytesGetter<T> getRowKeyGenerator();

    /**
     * Object to generate a millis-timestamp
     *
     * @return timestamp generator
     */
    HBaseLongGetter<T> getTimestampGenerator();

    /**
     * Object to generate a map of byte[] values
     *
     * @return bytes value generator
     */
    List<HBaseCellsMapper<T>> getPutGenerators();

    /**
     * Object to generate a map of increment deltas
     *
     * @return long increment generator
     */
    List<HBaseLongsMapper<T>> getIncrementGenerators();
}
