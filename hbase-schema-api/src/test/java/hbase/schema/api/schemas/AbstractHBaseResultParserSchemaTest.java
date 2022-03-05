package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseResultParserSchema;
import hbase.schema.api.testutils.DummyPojo;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

import static hbase.schema.api.converters.Utf8BytesMapConverter.utf8BytesMapConverter;
import static hbase.schema.api.utils.HBaseSchemaConversions.utf8ToBytes;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeMap;
import static hbase.schema.api.utils.HBaseSchemaUtils.asBytesTreeSet;
import static hbase.schema.api.utils.HBaseSchemaUtils.asStringMap;
import static hbase.test.utils.HBaseTestHelpers.fromUtf8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

class AbstractHBaseResultParserSchemaTest {
    HBaseResultParserSchema<DummyPojo> parser = new AbstractHBaseResultParserSchema<DummyPojo>() {
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
            obj.setId(fromUtf8(rowKey));
        }

        @Override
        public void setFromCell(DummyPojo obj, byte[] qualifier, byte[] value) {
            if (Bytes.equals(qualifier, utf8ToBytes("field"))) {
                obj.setString(fromUtf8(value));
            }
        }

        @Override
        public void setFromPrefix(DummyPojo obj, byte[] prefix, NavigableMap<byte[], byte[]> cellsFromPrefix) {
            if (Bytes.equals(prefix, utf8ToBytes("p1"))) {
                Map<String, String> map = utf8BytesMapConverter().fromBytesMap(cellsFromPrefix);
                obj.setMap1(map);
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
        parser.setFromResult(result, utf8ToBytes("row key"), resultCells);

        assertThat(result.getId(), equalTo("row key"));
        assertThat(result.getString(), equalTo("field value"));
        assertThat(result.getMap1(), equalTo(asStringMap("", "value", "b", "value b")));
        assertThat(result.getMap2(), nullValue());
    }
}
