package io.spbx.storage.bigqueue.page;

import io.spbx.util.base.error.BasicExceptions.IllegalArgumentExceptions;
import io.spbx.util.func.ScopeFunctions;
import io.spbx.util.io.BasicIo;
import io.spbx.util.io.UncheckedClosable;
import io.spbx.util.logging.Logger;

import javax.annotation.concurrent.ThreadSafe;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * Simple and thread-safe LRU cache implementation,
 * supporting time to live and reference counting for entry.
 * <p>
 * in current implementation, entry expiration and purge(mark&sweep) is triggered by put operation,
 * and resource closing after mark&sweep is done in async way.
 *
 * @param <K> key
 * @param <V> value
 */
@ThreadSafe
class LRUCacheImpl<K, V extends UncheckedClosable> implements LRUCache<K, V> {
    private static final Logger log = Logger.forEnclosingClass();

    private final ExecutorService executor;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<K, Entry<V>> map = new HashMap<>();

    public LRUCacheImpl(ExecutorService executor) {
        this.executor = executor;
    }

    public void put(K key, V value, long ttlMillis) {
        List<V> valuesToClose;
        try {
            lock.writeLock().lock();
            valuesToClose = markAndSweep();     // trigger mark&sweep before adding
            Entry<V> existing = map.putIfAbsent(key, new Entry<>(value, ttlMillis));
            IllegalArgumentExceptions.assure(existing == null, "Key already exists in cache:", key);
        } finally {
            lock.writeLock().unlock();
        }
        gc(valuesToClose);
    }

    @Override
    public int size() {
        try {
            lock.readLock().lock();
            return map.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public V get(K key) {
        try {
            lock.readLock().lock();
            return ScopeFunctions.map(map.get(key), entry -> {
                entry.acquire();
                return entry.value;
            });
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean contains(K key) {
        try {
            lock.readLock().lock();
            return map.containsKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void release(K key) {
        try {
            lock.readLock().lock();
            ScopeFunctions.let(map.get(key), Entry::release);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void removeAll() throws UncheckedIOException {
        try {
            lock.writeLock().lock();
            map.values().stream().map(e -> e.value).forEach(UncheckedClosable::close);  // close synchronously
            map.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public V remove(K key) throws UncheckedIOException {
        try {
            lock.writeLock().lock();
            return ScopeFunctions.map(map.remove(key), entry -> {
                BasicIo.Close.closeRethrow(entry.value);                                // close synchronously
                return entry.value;
            });
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void forEachValue(Consumer<V> consumer) {
        values().forEach(consumer);
    }

    List<V> values() {
        try {
            lock.readLock().lock();
            return map.values().stream().map(e -> e.value).toList();
        } finally {
            lock.readLock().unlock();
        }
    }

    void gc() {
        try {
            lock.writeLock().lock();
            gc(markAndSweep());
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void gc(List<V> values) {
        if (!values.isEmpty()) {
            log.info().log("Mark&Sweep found %d resources to close", values.size());
            executor.submit(() -> {
                values.forEach(BasicIo.Close::closeQuietly);                     // close resource asynchronously
                log.debug().log("ResourceCloser closed %d resources", values.size());
            });
        }
    }

    /**
     * A lazy mark and sweep, a separate thread can also do this.
     */
    private List<V> markAndSweep() {
        long now = now();
        List<K> keysToRemove = map.entrySet().stream().filter(e -> e.getValue().isExpired(now)).map(Map.Entry::getKey).toList();
        if (keysToRemove.isEmpty()) {
            return List.of();
        }

        List<V> valuesToClose = new ArrayList<>(keysToRemove.size());
        for (K key : keysToRemove) {
            Entry<V> entry = map.remove(key);
            valuesToClose.add(entry.value);
        }
        return valuesToClose;
    }

    private static class Entry<V> {
        private final V value;
        private final long ttl;
        private final AtomicLong lastAccess = new AtomicLong(now());
        private final AtomicLong refCount = new AtomicLong(1);          // auto-increment upon creation.

        public Entry(V value, long ttl) {
            this.value = value;
            this.ttl = ttl;
        }

        // Updates last accessed timestamp and increment reference counter
        void acquire() {
            lastAccess.set(now());
            refCount.incrementAndGet();
        }

        // Decrements the reference counter
        void release() {
            refCount.decrementAndGet();
        }

        boolean isExpired(long now) {
            return refCount.get() <= 0 && now - lastAccess.get() > ttl;       // no reference and expired
        }
    }

    private static long now() {
        return System.currentTimeMillis();
    }
}
