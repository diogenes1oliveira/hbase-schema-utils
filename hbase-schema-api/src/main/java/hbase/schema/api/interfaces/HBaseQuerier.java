package hbase.schema.api.interfaces;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;

import java.util.List;
import java.util.Map;

public interface HBaseQuerier {
    Get toGet(Map<String, String> params);

    List<Scan> toScans(Map<String, String> params);
}
