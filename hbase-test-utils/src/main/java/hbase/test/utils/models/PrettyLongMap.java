package hbase.test.utils.models;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Wraps over a {@code Map<byte[], Long>}, providing a printable {@link Object#toString()} and a
 * proper {@link Object#equals(Object)} implementation
 */
public class PrettyLongMap {
    private final TreeMap<byte[], Long> wrapped;

    /**
     * @param map input bytes map
     */
    public PrettyLongMap(@Nullable Map<byte[], Long> map) {
        if (map == null) {
            wrapped = null;
        } else {
            wrapped = new TreeMap<>(Bytes.BYTES_COMPARATOR);
            wrapped.putAll(map);
        }
    }

    /**
     * Original map passed to the constructor
     */
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
