package hbase.schema.api.testutils;

import hbase.schema.api.interfaces.HBaseMutationMapper;
import hbase.schema.api.interfaces.HBaseResultParser;
import hbase.schema.api.interfaces.HBaseSchema;
import hbase.schema.api.schemas.HBaseMutationMapperBuilder;
import hbase.schema.api.schemas.HBaseResultParserBuilder;

import static hbase.schema.api.converters.InstantLongConverter.instantLongConverter;
import static hbase.schema.api.converters.InstantStringConverter.instantStringConverter;
import static hbase.schema.api.converters.Utf8BytesMapConverter.utf8BytesMapConverter;
import static hbase.schema.api.converters.Utf8Converter.utf8Converter;
import static hbase.schema.api.interfaces.conversion.LongConverter.longConverter;
import static hbase.schema.api.interfaces.conversion.LongMapConverter.longMapKeyConverter;

public class DummyPojoSchema implements HBaseSchema<DummyPojo, DummyPojo> {
    @Override
    public HBaseMutationMapper<DummyPojo> mutationMapper() {
        return new HBaseMutationMapperBuilder<DummyPojo>()
                .timestamp(DummyPojo::getInstant, instantLongConverter())
                .rowKey(DummyPojo::getId, utf8Converter())
                .columnBytes("bytes", DummyPojo::getBytes)
                .column("string", DummyPojo::getString, utf8Converter())
                .column("long", DummyPojo::getLong, longConverter())
                .column("instant", DummyPojo::getInstant, instantStringConverter())
                .prefix("map1-", DummyPojo::getMap1, utf8BytesMapConverter())
                .prefix("map2-", DummyPojo::getMap2, longMapKeyConverter(utf8Converter()))
                .build();
    }

    @Override
    public HBaseResultParser<DummyPojo> resultParser() {
        return new HBaseResultParserBuilder<>(DummyPojo::new)
                .rowKey(DummyPojo::setId, utf8Converter())
                .columnBytes("bytes", DummyPojo::setBytes)
                .column("string", DummyPojo::setString, utf8Converter())
                .column("long", DummyPojo::setLong, longConverter())
                .column("instant", DummyPojo::setInstant, instantStringConverter())
                .prefix("map1-", DummyPojo::setMap1, utf8BytesMapConverter())
                .prefix("map2-", DummyPojo::setMap2, longMapKeyConverter(utf8Converter()))
                .build();
    }

    @Override
    public int scanKeySize() {
        return 3;
    }

}
