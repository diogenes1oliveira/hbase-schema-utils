package testutils;

import hbase.schema.api.interfaces.converters.HBaseBytesMapper;
import hbase.schema.api.interfaces.converters.HBaseBytesParser;
import hbase.schema.api.interfaces.converters.HBaseLongMapper;
import hbase.schema.api.interfaces.converters.HBaseLongParser;
import hbase.schema.api.schemas.AbstractHBasePojoWriteSchema;
import org.apache.hadoop.hbase.util.Triple;

import java.util.List;

import static hbase.schema.api.utils.HBaseBytesMappingUtils.stringMapper;

public class DummyPojoWriteSchema extends AbstractHBasePojoWriteSchema<DummyPojo> {
    @Override
    public List<Triple<String, HBaseBytesParser<DummyPojo>, HBaseBytesMapper<DummyPojo>>> getPojoValueFields() {
        return null;
    }

    @Override
    public List<Triple<String, HBaseLongParser<DummyPojo>, HBaseLongMapper<DummyPojo>>> getPojoDeltaFields() {
        return null;
    }

    @Override
    public HBaseBytesMapper<DummyPojo> getRowKeyGenerator() {
        return stringMapper(DummyPojo::getId);
    }

    @Override
    public HBaseLongMapper<DummyPojo> getTimestampGenerator() {
        return null;
    }
}
