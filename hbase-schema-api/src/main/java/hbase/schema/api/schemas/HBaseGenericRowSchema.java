package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseMutationSchema;
import hbase.schema.api.interfaces.HBaseQuerySchema;
import hbase.schema.api.interfaces.HBaseResultParser;
import hbase.schema.api.interfaces.HBaseSchema;
import hbase.schema.api.models.HBaseGenericRow;

public class HBaseGenericRowSchema implements HBaseSchema<HBaseGenericRow, HBaseGenericRow> {
    private static final byte[] EMPTY = new byte[0];

    @Override
    public HBaseQuerySchema<HBaseGenericRow> querySchema() {
        return new HBaseQuerySchemaBuilder<HBaseGenericRow>()
                .withRowKey(HBaseGenericRow::getRowKey)
                .withPrefixes(EMPTY)
                .build();
    }

    @Override
    public HBaseMutationSchema<HBaseGenericRow> mutationSchema() {
        return new HBaseMutationSchemaBuilder<HBaseGenericRow>()
                .withRowKey(HBaseGenericRow::getRowKey)
                .withTimestamp(HBaseGenericRow::getTimestampMs)
                .build();
    }

    @Override
    public HBaseResultParser<HBaseGenericRow> resultParser() {
        return null;
    }
}
