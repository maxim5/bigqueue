package io.spbx.storage.bigqueue.page;

import io.spbx.util.base.annotate.CanIgnoreReturnValue;
import io.spbx.util.io.UncheckedClosable;
import io.spbx.util.logging.Logger;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@SuppressWarnings("resource")
public class FakeLRUCache<K, V extends UncheckedClosable> implements LRUCache<K, V> {
    private static final Logger log = Logger.forEnclosingClass();

    private final ConcurrentHashMap<K, Entry<V>> map = new ConcurrentHashMap<>();

    @Override
    public void put(K key, V value, long ttlMillis) {
        Entry<V> prev = map.put(key, new Entry<>(value, ttlMillis));
        assert prev == null;
    }

    @Override
    public V get(K key) {
        return cleanup(key, entry -> {
            entry.lastAccess.set(now());
            entry.refCount.incrementAndGet();
        });
    }

    @Override
    public void release(K key) {
        with(key, entry -> entry.refCount.decrementAndGet());
    }

    @Override
    public boolean contains(K key) {
        return map.contains(key);
    }

    @Override
    public void forEachValue(Consumer<V> consumer) {
        values().forEach(consumer);
    }

    List<V> values() {
        cleanupAll();
        return map.values().stream().map(Entry::value).toList();
    }

    @Override
    public V remove(K key) {
        return with(key, entry -> {
            map.remove(key);
            entry.value.close();
        });
    }

    @Override
    public void removeAll() {
        map.clear();
    }

    @Override
    public int size() {
        cleanupAll();
        return map.size();
    }

    @CanIgnoreReturnValue
    private @Nullable V with(K key, @NotNull Consumer<Entry<V>> consumer) {
        Entry<V> entry = map.get(key);
        if (entry == null) {
            return null;
        }
        consumer.accept(entry);
        return entry.value;
    }

    @CanIgnoreReturnValue
    private @Nullable V cleanup(K key, @NotNull Consumer<Entry<V>> consumer) {
        Entry<V> entry = map.get(key);
        if (entry == null) {
            return null;
        }
        log.ultra().log("FakeLRUCache.cleanup(%s): %s", key, entry);
        if (entry.isExpired()) {
            map.remove(key);
            entry.value.close();
            return null;
        } else {
            consumer.accept(entry);
        }
        return entry.value;
    }
    
    private void cleanupAll() {
        new ArrayList<>(map.keySet()).forEach(key -> cleanup(key, __ -> {}));
    }

    private record Entry<V>(V value, long ttlMillis, @NotNull AtomicLong lastAccess, @NotNull AtomicLong refCount) {
        Entry(V value, long ttlMillis) {
            this(value, ttlMillis, new AtomicLong(now()), new AtomicLong(1));
        }

        boolean isExpired() {
            return lastAccess.get() + ttlMillis < now() && refCount.get() <= 0;
        }
    }

    private static long now() {
        return System.currentTimeMillis();
    }
}
