package hbase.schema.api.interfaces;

import hbase.schema.api.interfaces.converters.HBaseBytesMapper;
import hbase.schema.api.interfaces.converters.HBaseBytesParser;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static hbase.schema.api.utils.HBaseSchemaUtils.frozenSortedByteSet;
import static java.util.Collections.singletonList;

/**
 * Interface to parse data from a HBase Result into a Map of byte values
 */
@FunctionalInterface
public interface HBaseBytesMapReadSchema<T> extends HBaseReadSchema<Map<byte[], byte[]>> {
    Set<byte[]> EMPTY = frozenSortedByteSet(new byte[0]);

    /**
     * Generates a row key based on the map values
     *
     * @param query object with data to generate a Get query
     * @return built row key
     */
    byte[] generateRowKey(Map<byte[], byte[]> query);

    /**
     * Gets the row key prefix size for Scan queries
     * <p>
     * The default implementation just delegates to the full row key
     *
     * @param query object with data to generate a Scan query
     * @return size in bytes to slice {@link #generateRowKey(Map)}
     */
    default int getSearchKeySize(Map<byte[], byte[]> query) {
        return generateRowKey(query).length;
    }

    /**
     * Populates the map with data from the row key
     *
     * @param output output map where to store the result
     * @param rowKey row key fetched from HBase
     */
    default void parseRowKey(Map<byte[], byte[]> output, byte[] rowKey) {
        // nothing to do by default
    }

    /**
     * Object to populate the POJO with data from the fetched row key
     *
     * @return row key parser
     */
    default HBaseBytesParser<Map<byte[], byte[]>> getRowKeyParser() {
        return this::parseRowKey;
    }

    /**
     * Object to generate the Get row key
     *
     * @return row key generator
     */
    default HBaseBytesMapper<Map<byte[], byte[]>> getRowKeyGenerator() {
        return this::generateRowKey;
    }

    /**
     * Object to generate the Scan row key prefix
     *
     * @return search key prefix generator
     */
    default HBaseBytesMapper<Map<byte[], byte[]>> getScanRowKeyGenerator() {
        return query -> Arrays.copyOf(generateRowKey(query), getSearchKeySize(query));
    }

    /**
     * Object to populate the POJO with data from the fetched qualifiers and values
     *
     * @return cells parser
     */
    default List<HBaseCellParser<Map<byte[], byte[]>>> getCellParsers() {
        return singletonList(Map::put);
    }

    /**
     * Set of qualifier prefixes to read data from in a Get or a Put
     *
     * @return set of qualifier prefix bytes
     */
    default Set<byte[]> getQualifierPrefixes(Map<byte[], byte[]> query) {
        return EMPTY;
    }

}
