package io.spbx.storage.bigqueue.page;

import io.spbx.util.collect.IntSize;
import io.spbx.util.io.UncheckedClosable;

import java.io.UncheckedIOException;
import java.util.Collection;

/**
 * LRU cache ADT.
 *
 * @param <K> the key
 * @param <V> the value
 */
interface LRUCache<K, V extends UncheckedClosable> extends IntSize {
    long DEFAULT_TTL_MILLIS = 10 * 1000;  // 10 seconds

    /**
     * Adds a keyed resource with specific {@code ttlMillis} into the cache.
     * <p>
     * This method increments the reference counter of the keyed resource.
     *
     * @param key       the key of the cached resource
     * @param value     the to be cached resource
     * @param ttlMillis time to live in milliseconds
     */
    void put(K key, V value, long ttlMillis);

    /**
     * Adds a keyed resource with {@link #DEFAULT_TTL_MILLIS} into the cache.
     * <p>
     * This call will increment the reference counter of the keyed resource.
     *
     * @param key   the key of the cached resource
     * @param value the to be cached resource
     */
    default void put(K key, V value) {
        this.put(key, value, DEFAULT_TTL_MILLIS);
    }

    /**
     * Returns the cached resource with specific {@code key}.
     * <p>
     * This call increments the reference counter of the keyed resource.
     *
     * @param key the key of the cached resource
     * @return cached resource if exists
     */
    V get(K key);

    /**
     * Returns all cached values.
     */
    Collection<V> getValues();

    /**
     * Releases the cached resource with specific {@code key}.
     * <p>
     * This call decrements the reference counter of the keyed resource.
     *
     * @param key the key of the cached resource
     */
    void release(K key);

    /**
     * Removes the resource with specific {@code key} from the cache and closes it synchronously.
     *
     * @param key the key of the cached resource
     * @return the removed resource if exists
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    V remove(K key) throws UncheckedIOException;

    /**
     * Removes all cached resource from the cache and closes them synchronously.
     *
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void removeAll() throws UncheckedIOException;
}
