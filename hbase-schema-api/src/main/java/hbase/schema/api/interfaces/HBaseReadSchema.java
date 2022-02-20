package hbase.schema.api.interfaces;

import edu.umd.cs.findbugs.annotations.Nullable;
import hbase.schema.api.interfaces.converters.HBaseBytesExtractor;
import hbase.schema.api.interfaces.converters.HBaseBytesParser;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import java.util.List;
import java.util.Set;
import java.util.SortedSet;

import static hbase.schema.api.utils.HBaseSchemaUtils.sortedByteSet;

/**
 * Interface to parse data from a HBase Result into a POJO object
 *
 * @param <T> POJO type
 */
public interface HBaseReadSchema<T> extends HBaseFilterGenerator<T> {
    /**
     * Object to populate the POJO with data from the fetched row key
     *
     * @return row key parser
     */
    HBaseBytesParser<T> getRowKeyParser();

    /**
     * Object to generate the Get row key
     *
     * @return row key generator
     */
    HBaseBytesExtractor<T> getRowKeyGenerator();

    /**
     * Object to generate the Scan row key prefix
     *
     * @return search key prefix generator
     */
    HBaseBytesExtractor<T> getScanRowKeyGenerator();

    /**
     * Object to populate the POJO with data from the fetched qualifiers and values
     *
     * @return cells parser
     */
    List<HBaseCellParser<T>> getCellParsers();

    /**
     * Set of fixed qualifiers to read data from in a Get or a Put
     *
     * @return set of qualifier bytes
     */
    default Set<byte[]> getQualifiers(T query) {
        return sortedByteSet();
    }

    /**
     * Set of qualifier prefixes to read data from in a Get or a Put
     *
     * @return set of qualifier prefix bytes
     */
    default Set<byte[]> getQualifierPrefixes(T query) {
        return sortedByteSet();
    }

    /**
     * Generates a filter based on the data from a POJO object
     * <p>
     * The default implementation generates a column filter based on {@link HBaseReadSchema#getQualifierPrefixes(T)}
     *
     * @param query POJO object to act as query source data
     * @return built filter or null
     */
    @Nullable
    @Override
    default Filter toFilter(T query) {
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (byte[] prefix : getQualifierPrefixes(query)) {
            if (prefix.length > 0) {
                Filter qualifierFilter = new ColumnPrefixFilter(prefix);
                list.addFilter(qualifierFilter);
            }
        }
        if (list.size() == 0) {
            return null;
        } else {
            return list;
        }
    }

}
