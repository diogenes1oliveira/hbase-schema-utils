package hbase.test.utils;

import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Properties;

@ExtendWith(HBaseTestJunitExtension.class)
public class HBaseTestJunitExtensionTest {
    @Test
    void dummy(Properties props, TableName t1, TableName t2) {
        int a = 1;
    }
}
