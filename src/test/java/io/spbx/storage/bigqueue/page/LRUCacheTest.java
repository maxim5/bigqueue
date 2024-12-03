package io.spbx.storage.bigqueue.page;

import io.spbx.util.io.UncheckedClosable;
import io.spbx.util.testing.concurrent.DirectExecutor;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static com.google.common.truth.Truth.assertThat;
import static io.spbx.storage.bigqueue.page.Resource.newResource;
import static io.spbx.storage.bigqueue.page.Resource.waitForExpire;
import static io.spbx.util.func.ScopeFunctions.also;
import static io.spbx.util.testing.TestingBasics.waitFor;

@Tag("fast")
public class LRUCacheTest {
    private static final long KEY = 100L;

    @ParameterizedTest @EnumSource(Scenario.class)
    public void trivial_short_lived_resource(Scenario scenario) {
        LRUCache<Long, Resource> cache = newCache(scenario);
        Resource resource = newResource();
        long ttl = 3L;

        cache.put(KEY, resource, ttl);
        assertThat(cache.size()).isEqualTo(1);
        assertThat(cache.get(KEY)).isSameInstanceAs(resource);
        assertThat(resource.isClosed()).isFalse();

        waitForExpire(ttl);
        assertThat(resource.isClosed()).isFalse();

        cache.release(KEY);
        cache.release(KEY);
        assertThat(resource.isClosed()).isFalse();

        waitForExpire(ttl);
        triggerMarkAndSweep(cache);
        assertThat(cache.size()).isEqualTo(0);
        assertThat(cache.get(KEY)).isNull();
        assertThat(resource.isClosed()).isTrue();
    }

    @ParameterizedTest @EnumSource(Scenario.class)
    public void trivial_long_lived_resource(Scenario scenario) {
        LRUCache<Long, Resource> cache = newCache(scenario);
        Resource resource = newResource();

        cache.put(KEY, resource);
        assertThat(cache.size()).isEqualTo(1);
        assertThat(resource.isClosed()).isFalse();

        waitFor(5);
        assertThat(resource.isClosed()).isFalse();

        cache.release(KEY);
        triggerMarkAndSweep(cache);

        waitFor(5);
        assertThat(cache.get(KEY)).isSameInstanceAs(resource);
        assertThat(resource.isClosed()).isFalse();

        also(cache.remove(KEY), res -> {
            assertThat(res).isSameInstanceAs(resource);
            assertThat(resource.isClosed()).isTrue();
        });
        assertThat(cache.size()).isEqualTo(0);
    }

    private static <K, V extends UncheckedClosable> @NotNull LRUCache<K, V> newCache(@NotNull Scenario scenario) {
        return switch (scenario) {
            case FAKE -> new FakeLRUCache<>();
            case PROD -> new LRUCacheImpl<>(DirectExecutor.newInstance());
        };
    }

    private static void triggerMarkAndSweep(@NotNull LRUCache<?, ?> cache) {
        if (cache instanceof LRUCacheImpl<?, ?> impl) {
            impl.gc();
        }
    }

    public enum Scenario {
        FAKE,
        PROD,
    }
}
