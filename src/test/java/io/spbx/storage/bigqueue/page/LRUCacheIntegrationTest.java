package io.spbx.storage.bigqueue.page;

import io.spbx.storage.bigqueue.RandomData;
import io.spbx.util.testing.TestingBasics;
import io.spbx.util.testing.concurrent.TestingThreads;
import io.spbx.util.testing.concurrent.ThreadWorker;
import io.spbx.util.testing.concurrent.TraceExecutor;
import io.spbx.util.testing.ext.CloseAllExtension;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;
import static io.spbx.storage.bigqueue.page.Resource.newResource;
import static io.spbx.storage.bigqueue.page.Resource.waitForExpire;
import static io.spbx.util.func.ScopeFunctions.also;

@Tag("slow") @Tag("integration")
public class LRUCacheIntegrationTest {
    @RegisterExtension private static final CloseAllExtension CLOSE_ALL = new CloseAllExtension();
    private static final RandomData random = RandomData.seededForEachCallSite();
    private static final long TTL = 100;

    @Test
    public void single_thread() {
        long ttl = 10;
        TraceExecutor executor = newExecutor();
        LRUCache<Integer, Resource> cache = new LRUCacheImpl<>(executor);

        Resource res1 = newResource();
        cache.put(1, res1, ttl);
        also(cache.get(1), value -> {
            assertThat(value).isNotNull();
            assertThat(value).isEqualTo(res1);
            assertThat(value.isClosed()).isFalse();
        });

        waitForExpire(executor, ttl);           // let 1 expire
        cache.put(2, newResource());            // trigger mark and sweep
        also(cache.get(1), value -> {
            assertThat(value).isNotNull();      // will not expire since there is reference count
            assertThat(value).isEqualTo(res1);
            assertThat(value.isClosed()).isFalse();
        });

        cache.release(1);                       // release first put
        cache.release(1);                       // release first get
        waitForExpire(executor, ttl);           // let 1 expire
        cache.put(3, newResource());            // trigger mark and sweep
        also(cache.get(1), value -> {
            assertThat(value).isNotNull();      // will not expire since there is reference count
            assertThat(value).isEqualTo(res1);
            assertThat(value.isClosed()).isFalse();
        });

        cache.release(1);                       // release second get
        cache.release(1);                       // release third get
        waitForExpire(executor, ttl);           // let 1 expire

        Resource res2 = newResource();
        cache.put(4, res2);                     // trigger mark and sweep
        assertThat(cache.get(1)).isNull();

        waitForExpire(executor, ttl);           // let the cleaner do the job
        assertThat(res1.isClosed()).isTrue();
        assertThat(cache.size()).isEqualTo(3);

        also(cache.remove(2), value -> {
            assertThat(value).isNotNull();
            assertThat(value.isClosed()).isTrue();
        });
        assertThat(cache.size()).isEqualTo(2);

        also(cache.remove(3), value -> {
            assertThat(value).isNotNull();
            assertThat(value.isClosed()).isTrue();
        });
        assertThat(cache.size()).isEqualTo(1);

        cache.removeAll();
        waitForExpire(executor, ttl);           // let the cleaner do the job
        assertThat(res2.isClosed()).isTrue();
        assertThat(cache.size()).isEqualTo(0);
    }

    @Test
    public void multiple_threads() {
        TraceExecutor executor = newExecutor();
        LRUCache<Integer, Resource> cache = new LRUCacheImpl<>(executor);
        int threadNum = 100;

        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new Worker(i, cache).newThread();
        }
        TestingThreads.startAll(threads);
        TestingThreads.joinAll(threads);
        assertThat(cache.size()).isEqualTo(0);
    }

    @Test
    public void multiple_threads_random() {
        TraceExecutor executor = newExecutor();
        LRUCache<Integer, Resource> cache = new LRUCacheImpl<>(executor);
        int threadNum = 100;
        int loop = 10;

        Resource[] resources = new Resource[threadNum];
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            resources[i] = newResource();
            cache.put(i, resources[i], TTL);
        }
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new RandomWorker(threadNum, loop, cache).newThread();
        }
        TestingThreads.startAll(threads);
        TestingThreads.joinAll(threads);

        for (int i = 0; i < threadNum; i++) {
            also(cache.get(i), resource -> {
                assertThat(resource).isNotNull();
                assertThat(resource.isClosed()).isFalse();
            });
            cache.release(i);
        }
        for (int i = 0; i < threadNum; i++) {
            assertThat(resources[i].isClosed()).isFalse();
        }
        for (int i = 0; i < threadNum; i++) {
            cache.release(i);                           // release put
        }

        TestingBasics.waitFor(5);                       // tiny sleep so that resources are not expired yet
        cache.put(threadNum + 1, newResource(), TTL);   // trigger mark and sweep

        for (int i = 0; i < threadNum; i++) {
            also(cache.get(i), resource -> {
                assertThat(resource).isNotNull();
                assertThat(resource.isClosed()).isFalse();
            });
            cache.release(i);
        }

        waitForExpire(TTL);                             // let the test objects expire
        Resource resource = newResource();
        cache.put(threadNum + 2, resource, TTL);        // trigger mark and sweep
        for (int i = 0; i < threadNum; i++) {
            assertThat(cache.get(i)).isNull();
        }

        waitForExpire(executor, TTL);                   // let the cleaner do the job
        for (int i = 0; i < threadNum; i++) {
            assertThat(resources[i].isClosed()).isTrue();
        }

        assertThat(cache.size()).isEqualTo(2);
        assertThat(resource.isClosed()).isFalse();

        cache.removeAll();
        waitForExpire(executor, TTL);
        assertThat(resource.isClosed()).isTrue();
        assertThat(cache.size()).isEqualTo(0);
    }

    private record Worker(int id, LRUCache<Integer, Resource> cache) implements ThreadWorker {
        private static final AtomicInteger extra = new AtomicInteger(1000_000);

        @Override public void run() {
            int ix1 = extra.getAndIncrement();
            int ix2 = extra.getAndIncrement();

            Resource resource = newResource();
            cache.put(id, resource, TTL);

            also(cache.get(id), res -> {
                assertThat(res).isEqualTo(resource);
                assertThat(res.isClosed()).isFalse();
            });

            cache.release(id);                          // release first put
            cache.release(id);                          // release first get

            waitForExpire(TTL);
            cache.put(ix1, newResource(), TTL);         // trigger mark&sweep
            assertThat(cache.get(id)).isNull();

            waitForExpire(TTL);                         // let the cleaner do the job
            assertThat(resource.isClosed()).isTrue();

            cache.release(ix1);
            cache.put(ix2, newResource(), TTL);         // trigger mark&sweep

            also(cache.remove(ix2), res -> {
                assertThat(res).isNotNull();
                assertThat(res.isClosed()).isTrue();
            });

            waitForExpire(TTL);                         // let the cleaner do the job
            assertThat(cache.get(ix1)).isNull();
        }
    }

    private record RandomWorker(int idLimit, int loop, LRUCache<Integer, Resource> cache) implements ThreadWorker {
        private static final AtomicInteger extra = new AtomicInteger(1000_000);

        @Override public void run() {
            for (int i = 0; i < loop; i++) {
                int id = random.randomInt(idLimit);
                int ix1 = extra.getAndIncrement();
                int ix2 = extra.getAndIncrement();
                int ix3 = extra.getAndIncrement();

                assertThat(cache.get(id)).isNotNull();

                cache.put(ix1, newResource(), TTL);
                cache.put(ix2, newResource(), TTL);
                cache.put(ix3, newResource(), TTL);
                cache.release(ix1);
                cache.release(ix2);
                also(cache.remove(ix3), resource -> {
                    assertThat(resource).isNotNull();
                    assertThat(resource.isClosed()).isTrue();
                });

                cache.release(id);
            }
        }
    }

    private static @NotNull TraceExecutor newExecutor() {
        return CLOSE_ALL.addCloseablePerTest(TraceExecutor.wrap(Executors.newCachedThreadPool()));
    }
}
