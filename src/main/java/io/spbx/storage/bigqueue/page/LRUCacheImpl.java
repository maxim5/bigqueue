package io.spbx.storage.bigqueue.page;

import io.spbx.util.io.BasicIo;
import io.spbx.util.io.UncheckedClosable;
import io.spbx.util.logging.Logger;

import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
class LRUCacheImpl<K, V extends UncheckedClosable> implements LRUCache<K, V> {
    private static final Logger log = Logger.forEnclosingClass();

    private final ExecutorService executor;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<K, V> map = new HashMap<>();
    private final Map<K, TTLValue> ttlMap = new HashMap<>();

    public LRUCacheImpl(ExecutorService executor) {
        this.executor = executor;
    }

    public void put(K key, V value, long ttlMillis) {
        Collection<V> valuesToClose;
        try {
            lock.writeLock().lock();
            // trigger mark&sweep
            valuesToClose = markAndSweep();
            if (valuesToClose != null && valuesToClose.contains(value)) { // just be cautious
                valuesToClose.remove(value);
            }
            map.put(key, value);
            TTLValue ttl = new TTLValue(System.currentTimeMillis(), ttlMillis);
            ttl.refCount.incrementAndGet();
            ttlMap.put(key, ttl);
        } finally {
            lock.writeLock().unlock();
        }
        if (valuesToClose != null && !valuesToClose.isEmpty()) {
            log.info().log("Mark&Sweep found %d resources to close.".formatted(valuesToClose.size()));
            executor.execute(new ValueCloser<>(valuesToClose));    // close resource asynchronously
        }
    }

    /**
     * A lazy mark and sweep, a separate thread can also do this.
     */
    private Collection<V> markAndSweep() {
        Collection<V> valuesToClose = null;
        Set<K> keysToRemove = new HashSet<>();
        Set<K> keys = ttlMap.keySet();
        long currentTS = System.currentTimeMillis();

        // remove object with no reference and expired
        for (K key : keys) {
            TTLValue ttl = ttlMap.get(key);
            if (ttl.refCount.get() <= 0 && (currentTS - ttl.lastAccessedTimestamp.get()) > ttl.ttl) {
                keysToRemove.add(key);
            }
        }

        if (!keysToRemove.isEmpty()) {
            valuesToClose = new HashSet<>();
            for (K key : keysToRemove) {
                V v = map.remove(key);
                valuesToClose.add(v);
                ttlMap.remove(key);
            }
        }

        return valuesToClose;
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
            TTLValue ttl = ttlMap.get(key);
            if (ttl != null) {
                // Since the resource is acquired by calling thread,
                // let's update last accessed timestamp and increment reference counting
                ttl.lastAccessedTimestamp.set(System.currentTimeMillis());
                ttl.refCount.incrementAndGet();
            }
            return map.get(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void release(K key) {
        try {
            lock.readLock().lock();
            TTLValue ttl = ttlMap.get(key);
            if (ttl != null) {
                // since the resource is released by calling thread
                // let's decrement the reference counting
                ttl.refCount.decrementAndGet();
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void removeAll() throws UncheckedIOException {
        try {
            lock.writeLock().lock();
            Collection<V> valuesToClose = new ArrayList<>(map.values());
            valuesToClose.forEach(UncheckedClosable::close);    // close synchronously
            map.clear();
            ttlMap.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public V remove(K key) throws UncheckedIOException {
        try {
            lock.writeLock().lock();
            ttlMap.remove(key);
            V value = map.remove(key);
            BasicIo.Close.closeRethrow(value);    // close synchronously
            return value;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Collection<V> getValues() {
        try {
            lock.readLock().lock();
            return new ArrayList<>(map.values());
        } finally {
            lock.readLock().unlock();
        }
    }

    private static class TTLValue {
        private final AtomicLong lastAccessedTimestamp;  // last accessed time
        private final AtomicLong refCount = new AtomicLong(0);
        private final long ttl;

        public TTLValue(long ts, long ttl) {
            this.lastAccessedTimestamp = new AtomicLong(ts);
            this.ttl = ttl;
        }
    }

    private static class ValueCloser<V extends UncheckedClosable> implements Runnable {
        private final Collection<V> valuesToClose;

        public ValueCloser(Collection<V> valuesToClose) {
            this.valuesToClose = valuesToClose;
        }

        public void run() {
            valuesToClose.forEach(BasicIo.Close::closeQuietly);
            log.debug().log("ResourceCloser closed %d resources", valuesToClose.size());
        }
    }
}
