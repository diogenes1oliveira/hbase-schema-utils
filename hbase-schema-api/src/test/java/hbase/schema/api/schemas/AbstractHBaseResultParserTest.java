package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseResultParser;
import hbase.schema.api.testutils.DummyPojo;
import hbase.test.utils.HBaseTestHelpers;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.util.NavigableMap;
import java.util.NavigableSet;

import static hbase.schema.api.testutils.HBaseUtils.asStringMap;
import static hbase.schema.api.utils.HBaseFunctionals.mapToTreeMap;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;
import static hbase.test.utils.HBaseTestHelpers.bytesToUtf8;
import static hbase.test.utils.HBaseTestHelpers.utf8ToBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

class AbstractHBaseResultParserTest {
    HBaseResultParser<DummyPojo> parser = new AbstractHBaseResultParser<DummyPojo>() {
        @Override
        public DummyPojo newInstance() {
            return new DummyPojo();
        }

        @Override
        public NavigableSet<byte[]> getPrefixes() {
            return asBytesTreeSet(new byte[]{'p', '1'});
        }

        @Override
        public void setFromRowKey(DummyPojo obj, byte[] rowKey) {
            obj.setId(bytesToUtf8(rowKey));
        }

        @Override
        public void setFromCell(DummyPojo obj, byte[] qualifier, byte[] value) {
            if (Bytes.equals(qualifier, utf8ToBytes("field"))) {
                obj.setField(bytesToUtf8(value));
            }
        }

        @Override
        public void setFromPrefix(DummyPojo obj, byte[] prefix, NavigableMap<byte[], byte[]> cellsFromPrefix) {
            if (Bytes.equals(prefix, utf8ToBytes("p1"))) {
                obj.setMap1(mapToTreeMap(
                        cellsFromPrefix,
                        HBaseTestHelpers::bytesToUtf8,
                        HBaseTestHelpers::bytesToUtf8
                ));
            }
        }
    };

    @Test
    void testWithPrefixes() {
        NavigableMap<byte[], byte[]> resultCells = asBytesTreeMap(
                utf8ToBytes("field"), utf8ToBytes("field value"),
                utf8ToBytes("p"), utf8ToBytes("nothing"),
                utf8ToBytes("p2"), utf8ToBytes("other"),
                utf8ToBytes("p1"), utf8ToBytes("value"),
                utf8ToBytes("p1b"), utf8ToBytes("value b")
        );

        DummyPojo result = parser.newInstance();
        parser.setFromRowKey(result, utf8ToBytes("row key"));
        parser.setFromResult(result, resultCells);

        assertThat(result.getId(), equalTo("row key"));
        assertThat(result.getField(), equalTo("field value"));
        assertThat(result.getMap1(), equalTo(asStringMap("", "value", "b", "value b")));
        assertThat(result.getMap2(), nullValue());
    }
}
