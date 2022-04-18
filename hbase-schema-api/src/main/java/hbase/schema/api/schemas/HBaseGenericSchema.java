package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseMutationMapper;
import hbase.schema.api.interfaces.HBaseQueryMapper;
import hbase.schema.api.interfaces.HBaseResultParser;
import hbase.schema.api.interfaces.HBaseSchema;
import hbase.schema.api.models.HBaseGenericRow;
import hbase.schema.api.models.HBaseValueCell;

public class HBaseGenericSchema implements HBaseSchema<HBaseGenericRow, HBaseGenericRow> {
    private static final byte[] EMPTY = new byte[0];

    @Override
    public String name() {
        return HBaseGenericSchema.class.getSimpleName();
    }

    @Override
    public HBaseMutationMapper<HBaseGenericRow> mutationMapper() {
        return new HBaseMutationMapperBuilder<HBaseGenericRow>()
                .timestamp(HBaseGenericRow::getTimestamp)
                .rowKey(HBaseGenericRow::getRowKey)
                .prefix(EMPTY, row -> HBaseValueCell.toCellsMap(row.getValueCells()))
                .build();
    }

    @Override
    public HBaseResultParser<HBaseGenericRow> resultParser() {
        return new HBaseResultParserBuilder<>(HBaseGenericRow::new)
                .rowKey(HBaseGenericRow::setRowKey)
                .prefix(EMPTY, (row, cells) -> row.getValueCells().addAll(cells),
                        cellsMap -> HBaseValueCell.fromPrefixMap(EMPTY, null, cellsMap)
                )
                .build();
    }

    @Override
    public HBaseQueryMapper<HBaseGenericRow> queryMapper() {
        return new HBaseQueryMapperBuilder<HBaseGenericRow>()
                .rowKey(HBaseGenericRow::getRowKey)
                .searchPrefix(HBaseGenericRow::getRowKey)
                .build();
    }
}
