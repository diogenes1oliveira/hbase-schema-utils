package hbase.test.utils;

import hbase.connector.HBaseConnector;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(HBaseTestJunitExtension.class)
public class HBaseTestJunitExtensionTest {
    @Test
    void dummy(HBaseConnector connector, TableName t1, TableName t2) {
        int a = 1;
    }
}
