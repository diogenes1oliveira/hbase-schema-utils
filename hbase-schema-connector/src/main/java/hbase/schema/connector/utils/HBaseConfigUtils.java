package hbase.schema.connector.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

public final class HBaseConfigUtils {
    private HBaseConfigUtils() {
        // utility class
    }

    public static List<Pair<String, String>> toKeyValuePairs(String input) {
        if (StringUtils.isBlank(input)) {
            return emptyList();
        }

        List<Pair<String, String>> pairs = new ArrayList<>();

        for (String part : input.split("[\\s,]+")) {
            String[] parts = part.trim().split("=");
            if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()) {
                continue;
            }
            pairs.add(Pair.of(parts[0], parts[1]));
        }

        return pairs;
    }
}
