package com.example.ksia.collectors;

import java.util.Comparator;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Class that acts as a priority queue but has a fixed size.
 * When the maximum number of elements is reached the lowest/highest element
 * will be removed.
 */
public class FixedSizePriorityQueue<T> {

    private final TreeSet<T> inner;

    private final int maxSize;

    public FixedSizePriorityQueue(Comparator<T> comparator, int maxSize) {
        this.inner = new TreeSet<>(comparator);
        this.maxSize = maxSize;
    }

    public void add(T element) {
        inner.add(element);
        if (inner.size() > maxSize) {
            inner.pollLast();
        }
    }

    public FixedSizePriorityQueue<T> remove(T element) {
        inner.remove(element);
        return this;
    }

    public Iterator<T> iterator() {
        return inner.iterator();
    }

    @Override
    public String toString() {
        return "FixedSizePriorityQueue{ QueueContents=" + inner + "}";

    }
}
