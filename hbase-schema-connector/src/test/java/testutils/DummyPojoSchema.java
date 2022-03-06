package testutils;

import hbase.schema.api.schemas.AbstractHBaseSchema;
import org.jetbrains.annotations.Nullable;

import static hbase.schema.api.converters.InstantLongConverter.instantLongConverter;
import static hbase.schema.api.converters.InstantStringConverter.instantStringConverter;
import static hbase.schema.api.converters.Utf8BytesMapConverter.utf8BytesMapConverter;
import static hbase.schema.api.converters.Utf8Converter.utf8Converter;
import static hbase.schema.api.interfaces.conversion.LongConverter.longConverter;
import static hbase.schema.api.interfaces.conversion.LongMapConverter.longMapKeyConverter;

public class DummyPojoSchema extends AbstractHBaseSchema<DummyPojo, DummyPojo> {
    public DummyPojoSchema() {
        withValue("bytes", DummyPojo::getBytes, DummyPojo::setBytes);
        withValue("string", DummyPojo::getString, DummyPojo::setString, utf8Converter());
        withValue("long", DummyPojo::getLong, DummyPojo::setLong, longConverter());
        withValue("instant", DummyPojo::getInstant, DummyPojo::setInstant, instantStringConverter());

        withValues("map1-", DummyPojo::getMap1, DummyPojo::setMap1, utf8BytesMapConverter());
        withDeltas("map2-", DummyPojo::getMap2, DummyPojo::setMap2, longMapKeyConverter(utf8Converter()));
    }

    @Override
    public DummyPojo newInstance() {
        return new DummyPojo();
    }

    @Override
    public @Nullable Long buildTimestamp(DummyPojo object) {
        return instantLongConverter().toLong(object.getInstant());
    }

    @Override
    public byte @Nullable [] buildRowKey(DummyPojo object) {
        return utf8Converter().toBytes(object.getId());
    }

    @Override
    public int scanKeySize() {
        return 3;
    }

    @Override
    public void parseRowKey(DummyPojo object, byte[] rowKey) {
        object.setId(utf8Converter().fromBytes(rowKey));
    }
}
