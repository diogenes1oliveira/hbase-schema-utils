package hbase.schema.connector.interfaces;

import hbase.schema.api.interfaces.HBaseCellParser;
import hbase.schema.api.interfaces.HBaseReadSchema;
import org.apache.hadoop.hbase.client.Result;

import java.util.Map;
import java.util.SortedMap;

public interface HBaseResultParser<T> {
    default void parse(T obj, byte[] rowKey, SortedMap<byte[], byte[]> cells, HBaseReadSchema<T> readSchema) {
        readSchema.getRowKeyParser().setFromBytes(obj, rowKey);

        for (HBaseCellParser<T> cellParser : readSchema.getCellParsers()) {
            for (Map.Entry<byte[], byte[]> entry : cells.entrySet()) {
                cellParser.parse(obj, entry.getKey(), entry.getValue());
            }
        }
    }

    T parse(Result result);

}
