package hbase.schema.api.interfaces;

import org.apache.hadoop.hbase.client.Result;

import java.util.Map;

public interface HBaseParser {
    Map<String, Object> parse(Result result);
}
