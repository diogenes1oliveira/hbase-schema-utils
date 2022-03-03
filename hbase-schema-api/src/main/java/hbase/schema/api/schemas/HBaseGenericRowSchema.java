package hbase.schema.api.schemas;

import hbase.schema.api.interfaces.HBaseMutationSchema;
import hbase.schema.api.interfaces.HBaseQuerySchema;
import hbase.schema.api.interfaces.HBaseResultParserSchema;
import hbase.schema.api.interfaces.HBaseSchema;
import hbase.schema.api.models.HBaseGenericRow;
import hbase.schema.api.models.HBaseValueCell;

import java.util.Map;
import java.util.NavigableMap;
import java.util.SortedSet;

import static hbase.schema.api.models.HBaseLongCell.longCellsToMap;
import static hbase.schema.api.models.HBaseValueCell.valueCellsToMap;

/**
 * Schema to fetch and put generic row data from/into HBase
 */
public class HBaseGenericRowSchema implements HBaseSchema<HBaseGenericRow, HBaseGenericRow> {
    private static final byte[] EMPTY = new byte[0];
    private final int searchKeySize;

    /**
     * @param searchKeySize search key size, row key will be cropped to this size in Scan queries
     */
    public HBaseGenericRowSchema(int searchKeySize) {
        this.searchKeySize = searchKeySize;
    }

    public HBaseGenericRowSchema() {
        this(-1);
    }

    /**
     * Schema to generate Gets and Scans
     */
    @Override
    public HBaseQuerySchema<HBaseGenericRow> querySchema() {
        HBaseQuerySchemaBuilder<HBaseGenericRow> builder = new HBaseQuerySchemaBuilder<HBaseGenericRow>()
                .withRowKey(HBaseGenericRow::getRowKey)
                .withPrefixes(EMPTY);
        if (searchKeySize != -1) {
            builder = builder.withScanKeySize(searchKeySize);
        }
        return builder.build();
    }

    /**
     * Schema to generate mutations
     */
    @Override
    public HBaseMutationSchema<HBaseGenericRow> mutationSchema() {
        return new HBaseMutationSchemaBuilder<HBaseGenericRow>()
                .withRowKey(HBaseGenericRow::getRowKey)
                .withTimestamp(HBaseGenericRow::getTimestamp)
                .withValues(EMPTY, obj -> valueCellsToMap(obj.getValueCells()))
                .withDeltas(EMPTY, obj -> longCellsToMap(obj.getLongCells()))
                .build();
    }

    /**
     * Schema to parse query results
     */
    @Override
    public HBaseResultParserSchema<HBaseGenericRow> resultParserSchema() {
        return new HBaseResultParserBuilder<>(HBaseGenericRow::new)
                .fromRowKey(HBaseGenericRow::setRowKey)
                .fromPrefix(EMPTY, HBaseGenericRowSchema::setCells)
                .build();
    }

    private static void setCells(HBaseGenericRow genericRow, NavigableMap<byte[], byte[]> cellsMap) {
        SortedSet<HBaseValueCell> cells = genericRow.getValueCells();

        for (Map.Entry<byte[], byte[]> entry : cellsMap.entrySet()) {
            HBaseValueCell cell = new HBaseValueCell(
                    entry.getKey(),
                    entry.getValue(),
                    null
            );
            cells.add(cell);
        }
    }
}
