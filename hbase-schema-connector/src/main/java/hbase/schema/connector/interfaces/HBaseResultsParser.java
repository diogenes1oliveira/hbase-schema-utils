package hbase.schema.connector.interfaces;

import org.apache.hadoop.hbase.client.Result;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Parses fetched HBase results into actual Java objects
 *
 * @param <T> result object type
 */
@FunctionalInterface
public interface HBaseResultsParser<T> {
    /**
     * Parses HBase results into Java objects
     *
     * @param result HBase result
     * @return parsed result object or null if no object could be parsed from the result
     */
    @Nullable
    T parseResult(Result result);

    /**
     * Parses HBase results from an iterator
     * <p>
     * The default implementation calls {@link #parseResult(Result)}, skipping null objects
     *
     * @param results iterator over HBase results
     * @return list of non-null parsed results
     */
    default List<T> parseResults(Iterator<Result> results) {
        List<T> objects = new ArrayList<>();

        while (results.hasNext()) {
            T object = parseResult(results.next());
            if (object != null) {
                objects.add(object);
            }
        }

        return objects;
    }

    /**
     * Parses HBase results from an iterable
     * <p>
     * The default implementation forwards to {@link #parseResults(Iterator)}
     *
     * @param results iterable of HBase results
     * @return list of non-null parsed results
     */
    default List<T> parseResults(Iterable<Result> results) {
        return parseResults(results.iterator());
    }

}
