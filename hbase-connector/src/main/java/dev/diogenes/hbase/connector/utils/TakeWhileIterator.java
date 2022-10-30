package dev.diogenes.hbase.connector.utils;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Wraps an iterator in a new iterator that stops when a given predicate returns true
 *
 * @param <T> item type
 */
public class TakeWhileIterator<T> implements Iterator<T> {
    // Taken from https://coderanch.com/t/664547/java/Java-data-directed#3093922
    private static final int NO_ADDITIONAL_CHARACTERISTICS = 0;

    private final Iterator<? extends T> inner;
    private final Predicate<? super T> predicate;

    private boolean innerHadNext;
    private T next;

    /**
     * @param inner     wrapped iterator
     * @param predicate stop condition
     */
    public TakeWhileIterator(Iterator<? extends T> inner, Predicate<? super T> predicate) {
        if (inner == null || predicate == null)
            throw new IllegalArgumentException();

        this.inner = inner;
        this.predicate = predicate;
        prepareNext();
    }

    private void prepareNext() {
        innerHadNext = inner.hasNext();
        next = innerHadNext ? inner.next() : null;
    }

    @Override
    public boolean hasNext() {
        return innerHadNext && predicate.test(next);
    }

    @Override
    public T next() {
        if (!hasNext())
            throw new NoSuchElementException();

        T result = next;
        prepareNext();
        return result;
    }

    /**
     * Yields a new stream that yields data from the input stream until a stop condition is met
     *
     * @param stream    original stream
     * @param predicate stop condition
     * @param <T>       item type
     * @return new filtered stream
     */
    public static <T> Stream<T> streamTakeWhile(Stream<T> stream, Predicate<T> predicate) {
        Supplier<Spliterator<T>> supplier = () -> Spliterators.spliteratorUnknownSize(
                new TakeWhileIterator<>(stream.iterator(), predicate),
                NO_ADDITIONAL_CHARACTERISTICS
        );

        return StreamSupport.stream(supplier, NO_ADDITIONAL_CHARACTERISTICS, false);
    }
}
