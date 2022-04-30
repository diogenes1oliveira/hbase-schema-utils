package testutils;

import static hbase.schema.api.interfaces.conversion.LongConverter.longConverter;

public class DummyPojoSchema {//implements HBaseSchema<DummyPojo, DummyPojo> {
//    @Override
//    public HBaseMutationMapper<DummyPojo> mutationMapper() {
//        return new HBaseMutationMapperBuilder<DummyPojo>()
//                .timestamp(DummyPojo::getInstant, instantLongConverter())
//                .rowKey(DummyPojo::getId, utf8Converter())
//                .column("bytes", DummyPojo::getBytes)
//                .column("string", DummyPojo::getString, utf8Converter())
//                .column("long", DummyPojo::getLong, longConverter())
//                .column("instant", DummyPojo::getInstant, instantStringConverter())
//                .prefix("map1-", DummyPojo::getMap1, utf8BytesMapConverter())
//                .prefix("map2-", DummyPojo::getMap2, longMapKeyConverter(utf8Converter()))
//                .build();
//    }
//
//    @Override
//    public HBaseResultParser<DummyPojo> resultParser() {
//        return new HBaseResultParserBuilder<>(DummyPojo::new)
//                .rowKey(DummyPojo::setId, utf8Converter())
//                .column("bytes", DummyPojo::setBytes)
//                .column("string", DummyPojo::setString, utf8Converter())
//                .column("long", DummyPojo::setLong, longConverter())
//                .column("instant", DummyPojo::setInstant, instantStringConverter())
//                .prefix("map1-", DummyPojo::setMap1, utf8BytesMapConverter())
//                .prefix("map2-", DummyPojo::setMap2, longMapKeyConverter(utf8Converter()))
//                .build();
//    }
//
//    @Override
//    public HBaseQueryMapper<DummyPojo> queryMapper() {
//        return new HBaseQueryMapperBuilder<DummyPojo>()
//                .rowKey(DummyPojo::getId, utf8Converter())
//                .searchKeySlice(3)
//                .columns("bytes", "string", "long", "instant")
//                .prefixes("map1-", "map2-")
//                .build();
//    }

}
