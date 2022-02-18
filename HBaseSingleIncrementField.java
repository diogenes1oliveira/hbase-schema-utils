package com.github.diogenes1oliveira.hbase.schema;

import com.github.diogenes1oliveira.hbase.utils.PayloadUtils;
import com.github.diogenes1oliveira.utils.functional.ThrowingBiConsumer;
import com.github.diogenes1oliveira.utils.functional.ThrowingFunction;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

public class HBaseSingleIncrementField<I, O> implements HBaseField<I, O> {
    private final byte[] qualifier;

    private final ThrowingFunction<I, Long, IOException> getter;
    private final ThrowingBiConsumer<O, Long, IOException> setter;

    public HBaseSingleIncrementField(
            byte[] qualifier,
            ThrowingFunction<I, Long, IOException> getter,
            ThrowingBiConsumer<O, Long, IOException> setter
    ) {
        this.qualifier = qualifier;
        this.getter = getter;
        this.setter = setter;
    }

    @Override
    public byte[] qualifier() {
        return qualifier;
    }

    @Override
    public boolean increment() {
        return true;
    }

    @Override
    public boolean reversed() {
        return false;
    }

    @Override
    public Map<byte[], byte[]> valueToCells(I input) throws IOException {
        Map<byte[], byte[]> cells = PayloadUtils.emptyByteMap();

        Long amount = getter.apply(input);
        if (amount == null || amount == 0) {
            return cells;
        }

        cells.put(qualifier, Bytes.toBytes(amount));
        return cells;
    }

    @Override
    public void cellsToValue(O output, Map<byte[], byte[]> cells) throws IOException {
        byte[] data = cells.get(qualifier);
        if (data == null) {
            return;
        }

        long value = PayloadUtils.bytesToLong(data);
        setter.accept(output, value);
    }
}
