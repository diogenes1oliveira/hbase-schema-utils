package com.github.diogenes1oliveira.hbase.schema.interfaces;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

import static com.github.diogenes1oliveira.hbase.schema.utils.HBaseUtils.getCellQualifier;
import static com.github.diogenes1oliveira.hbase.schema.utils.HBaseUtils.getCellValue;

/**
 * Interface to populate a POJO with data fetched from HBase
 *
 * @param <T> POJO type
 */
public interface HBaseResultParser<T> {

    /**
     * Populates the POJO with data from a cell
     *
     * @param pojo      POJO object
     * @param qualifier column qualifier
     * @param value     cell value
     * @param family    column family
     */
    void parseCell(T pojo, byte[] qualifier, byte[] value, byte[] family);

    /**
     * Populates the POJO with data from the row key
     *
     * @param pojo   POJO object
     * @param rowKey row key binary data
     * @param family column family
     */
    default void parseRowKey(T pojo, byte[] rowKey, byte[] family) {
        // nothing to do by default
    }

    /**
     * Populates the POJO with data from the row key, qualifiers and values fetched from HBase
     * <p>
     * This default implementation forwards to {@link #parseRowKey(T, byte[], byte[])} and
     * {@link #parseCells(T, Result, byte[])}
     *
     * @param pojo   POJO object
     * @param result HBase result item
     * @param family column family
     */
    default void parseResult(T pojo, Result result, byte[] family) throws IOException {
        byte[] rowKey = result.getRow();
        if (rowKey == null) {
            // I don't think this is possible, but just in case
            return;
        }
        parseRowKey(pojo, rowKey, family);
        parseCells(pojo, result, family);
    }

    /**
     * Populates the POJO with data from the cell
     * <p>
     * This default implementation extracts the cell data and forwards it into {@link #parseCell(T, byte[], byte[], byte[])}
     *
     * @param pojo   POJO object
     * @param result HBase result item
     * @param family column family
     */
    default void parseCells(T pojo, Result result, byte[] family) throws IOException {
        CellScanner scanner = result.cellScanner();
        if (scanner == null) {
            return;
        }
        while (scanner.advance()) {
            Cell cell = scanner.current();
            if (cell == null) {
                continue;
            }
            byte[] qualifier = getCellQualifier(cell);
            byte[] value = getCellValue(cell);
            if (qualifier != null && value != null) {
                parseCell(pojo, qualifier, value, family);
            }
        }
    }

}
