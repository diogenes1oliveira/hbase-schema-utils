package testutils;

import hbase.schema.api.interfaces.HBaseMutationSchema;
import hbase.schema.api.interfaces.HBaseQuerySchema;
import hbase.schema.api.interfaces.HBaseResultParserSchema;
import hbase.schema.api.interfaces.HBaseSchema;
import hbase.schema.api.schemas.HBaseMutationSchemaBuilder;
import hbase.schema.api.schemas.HBaseQuerySchemaBuilder;
import hbase.schema.api.schemas.HBaseResultParserSchemaBuilder;
import org.apache.hadoop.hbase.util.Bytes;

import java.time.Instant;

import static hbase.schema.api.interfaces.converters.HBaseBytesMapGetter.bytesMapGetter;
import static hbase.schema.api.interfaces.converters.HBaseBytesMapSetter.bytesMapSetter;
import static hbase.schema.api.interfaces.converters.HBaseLongGetter.longGetter;
import static hbase.schema.api.interfaces.converters.HBaseLongMapGetter.longMapGetter;
import static hbase.schema.api.utils.HBaseSchemaConversions.longSetter;
import static hbase.schema.api.utils.HBaseSchemaConversions.stringGetter;
import static hbase.schema.api.utils.HBaseSchemaConversions.stringSetter;
import static hbase.schema.api.utils.HBaseSchemaConversions.utf8FromBytes;
import static hbase.schema.api.utils.HBaseSchemaConversions.utf8ToBytes;

public class DummyPojoSchema implements HBaseSchema<DummyPojo, DummyPojo> {
    public static final int SCAN_KEY_SIZE = 3;

    @Override
    public HBaseQuerySchema<DummyPojo> querySchema() {
        return new HBaseQuerySchemaBuilder<DummyPojo>()
                .withRowKey(stringGetter(DummyPojo::getId))
                .withScanKeySize(SCAN_KEY_SIZE)
                .withQualifiers("bytes", "string", "long", "instant")
                .withPrefixes("map1-", "map2-")
                .build();
    }

    @Override
    public HBaseMutationSchema<DummyPojo> mutationSchema() {
        return new HBaseMutationSchemaBuilder<DummyPojo>()
                .withTimestamp(longGetter(DummyPojo::getInstant, Instant::toEpochMilli))
                .withRowKey(stringGetter(DummyPojo::getId))
                .withValues("map1-", bytesMapGetter(DummyPojo::getMap1, utf8ToBytes(), utf8ToBytes()))
                .withDeltas("map2-", longMapGetter(DummyPojo::getMap2, utf8ToBytes()))
                .withValue("bytes", DummyPojo::getBytes)
                .withValue("string", stringGetter(DummyPojo::getString))
                .withValue("long", longGetter(DummyPojo::getLong))
                .withValue("instant", stringGetter(DummyPojo::getInstant, Instant::toString))
                .build();
    }

    @Override
    public HBaseResultParserSchema<DummyPojo> resultParserSchema() {
        return new HBaseResultParserSchemaBuilder<>(DummyPojo::new)
                .fromRowKey(stringSetter(DummyPojo::setId))
                .fromPrefix("map1-", bytesMapSetter(DummyPojo::setMap1, utf8FromBytes(), utf8FromBytes()))
                .fromPrefix("map2-", bytesMapSetter(DummyPojo::setMap2, utf8FromBytes(), Bytes::toLong))
                .fromColumn("bytes", DummyPojo::setBytes)
                .fromColumn("string", stringSetter(DummyPojo::setString))
                .fromColumn("long", longSetter(DummyPojo::setLong))
                .fromColumn("instant", stringSetter(DummyPojo::setInstant, Instant::parse))
                .build();
    }
}
