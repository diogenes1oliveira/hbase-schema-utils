package hbase.schema.connector.services;

import hbase.schema.api.interfaces.HBaseResultParserSchema;
import hbase.schema.connector.interfaces.HBaseResultsParser;
import org.apache.hadoop.hbase.client.Result;
import org.jetbrains.annotations.Nullable;

import java.util.NavigableMap;

/**
 * Parses fetched HBase results into actual Java objects using a schema
 *
 * @param <T> result object type
 */
public class HBaseSchemaResultsParser<T> implements HBaseResultsParser<T> {
    private final byte[] family;
    private final HBaseResultParserSchema<T> resultParser;

    /**
     * @param family       column family
     * @param resultParser result parser
     */
    public HBaseSchemaResultsParser(byte[] family, HBaseResultParserSchema<T> resultParser) {
        this.family = family;
        this.resultParser = resultParser;
    }

    /**
     * Parses HBase results into Java objects using the schema
     *
     * @param result HBase result
     * @return parsed result object or null if no object could be parsed from the result
     */
    @Nullable
    @Override
    public T parseResult(Result result) {
        if (result == null) {
            return null;
        }

        byte[] rowKey = result.getRow();
        NavigableMap<byte[], byte[]> cellsMap = result.getFamilyMap(family);

        if (rowKey == null || cellsMap == null || cellsMap.isEmpty()) {
            return null;
        }

        T object = resultParser.newInstance();
        resultParser.setFromResult(object, rowKey, cellsMap);
        return object;
    }

}
