package hbase.schema.api.models;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

public class PrettyLongMap {
    private final TreeMap<byte[], Long> wrapped;

    public PrettyLongMap(@Nullable Map<byte[], Long> map) {
        if (map == null) {
            wrapped = null;
        } else {
            wrapped = new TreeMap<>(Bytes.BYTES_COMPARATOR);
            wrapped.putAll(map);
        }
    }

    @Nullable
    public SortedMap<byte[], Long> getWrapped() {
        return wrapped;
    }

    @Override
    public String toString() {
        if (wrapped == null) {
            return Objects.toString(null);
        }

        Map<String, String> prettyMap = new HashMap<>();
        for (Map.Entry<byte[], Long> entry : wrapped.entrySet()) {
            byte[] key = entry.getKey();
            String prettyKey = Bytes.toStringBinary(key);
            Long value = entry.getValue();
            String prettyValue = Objects.toString(value);
            prettyMap.put(prettyKey, prettyValue);
        }

        return Objects.toString(prettyMap);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PrettyLongMap)) {
            return false;
        }
        PrettyLongMap other = (PrettyLongMap) obj;
        return Objects.equals(this.wrapped, other.wrapped);
    }

}
