package hbase.schema.api.interfaces;

import hbase.schema.api.models.HBaseRowRange;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

@FunctionalInterface
public interface HBaseRowRangeMapper {
    List<HBaseRowRange> toRanges(Map<String, String> params);

    static HBaseRowRangeMapper bucketsMapper(List<byte[]> buckets) {
        return params -> {
            List<HBaseRowRange> ranges = new ArrayList<>();

            for (byte[] bucket : buckets) {
                HBaseRowRange range = new HBaseRowRange(bucket);
                ranges.add(range);
            }

            return ranges;
        };
    }

    static HBaseRowRangeMapper constantMapper(byte[] value) {
        return params -> {
            HBaseRowRange range = new HBaseRowRange(value);
            return singletonList(range);
        };
    }

    static HBaseRowRangeMapper paramMapper(String paramName, BytesMapper<String> prefixMapper) {
        return params -> {
            String paramValue = params.get(paramName);
            byte[] prefix = prefixMapper.toBytes(paramValue);
            HBaseRowRange range;
            if (prefix == null) {
                range = new HBaseRowRange(null, null);
            } else {
                range = new HBaseRowRange(prefix);
            }
            return singletonList(range);
        };
    }

    static HBaseRowRangeMapper paramMapper(String paramName, BytesMapper<String> startMapper, BytesMapper<String> stopMapper) {
        return params -> {
            String paramValue = params.get(paramName);
            byte[] start = startMapper.toBytes(paramValue);
            byte[] stop = stopMapper.toBytes(paramValue);

            HBaseRowRange range = new HBaseRowRange(start, stop);
            return singletonList(range);
        };
    }

}
