package io.spbx.storage.bigqueue.page;

import io.spbx.storage.bigqueue.Timeouts;
import io.spbx.util.io.UncheckedClosable;
import io.spbx.util.testing.TestingBasics;
import io.spbx.util.testing.TestingThreads;
import io.spbx.util.testing.TestingThreads.ThreadWorker;
import io.spbx.util.testing.ext.CloseAllExtension;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.truth.Truth.assertThat;
import static io.spbx.util.func.ScopeFunctions.also;

@Tag("slow") @Tag("integration")
public class LRUCacheIntegrationTest {
    @RegisterExtension private static final CloseAllExtension CLOSE_ALL = new CloseAllExtension();
    private static final int TIMEOUT_SMALL = Timeouts.timeout(600, 1000);
    private static final int TIMEOUT_MEDIUM = Timeouts.timeout(1000, 1000);
    private static final int TIMEOUT_FULL = Timeouts.timeout(2000 + 10, 2500);

    @Test
    public void single_thread() {
        LRUCache<Integer, TestObject> cache = new LRUCacheImpl<>(newExecutor());

        TestObject obj1 = new TestObject();
        cache.put(1, obj1, 500);
        also(cache.get(1), value -> {
            assertThat(value).isNotNull();
            assertThat(value).isEqualTo(obj1);
            assertThat(value.isClosed()).isFalse();
        });

        TestingBasics.waitFor(TIMEOUT_SMALL);   // let 1 expire
        cache.put(2, new TestObject());         // trigger mark and sweep
        also(cache.get(1), value -> {
            assertThat(value).isNotNull();      // will not expire since there is reference count
            assertThat(value).isEqualTo(obj1);
            assertThat(value.isClosed()).isFalse();
        });

        cache.release(1);                       // release first put
        cache.release(1);                       // release first get
        TestingBasics.waitFor(TIMEOUT_SMALL);   // let 1 expire
        cache.put(3, new TestObject());         // trigger mark and sweep
        also(cache.get(1), value -> {
            assertThat(value).isNotNull();      // will not expire since there is reference count
            assertThat(value).isEqualTo(obj1);
            assertThat(value.isClosed()).isFalse();
        });

        cache.release(1);                       // release second get
        cache.release(1);                       // release third get
        TestingBasics.waitFor(TIMEOUT_SMALL);   // let 1 expire

        TestObject obj2 = new TestObject();
        cache.put(4, obj2);                     // trigger mark and sweep
        assertThat(cache.get(1)).isNull();

        TestingBasics.waitFor(TIMEOUT_SMALL);   // let the cleaner do the job
        assertThat(obj1.isClosed()).isTrue();
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
        TestingBasics.waitFor(TIMEOUT_SMALL);   // let the cleaner do the job
        assertThat(obj2.isClosed()).isTrue();
        assertThat(cache.size()).isEqualTo(0);
    }

    @Test
    public void multiple_threads() {
        LRUCache<Integer, TestObject> cache = new LRUCacheImpl<>(newExecutor());
        int threadNum = 100;

        // Run workers
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
        LRUCache<Integer, TestObject> cache = new LRUCacheImpl<>(newExecutor());
        int threadNum = 100;

        // Run workers
        Thread[] threads = new Thread[threadNum];
        TestObject[] testObjects = new TestObject[threadNum];
        for (int i = 0; i < threadNum; i++) {
            testObjects[i] = new TestObject();
            cache.put(i, testObjects[i], 2000);
        }
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new RandomWorker(threadNum, cache, new Random()).newThread();
        }
        TestingThreads.startAll(threads);
        TestingThreads.joinAll(threads);

        // verification
        for (int i = 0; i < threadNum; i++) {
            TestObject testObj = cache.get(i);
            assertThat(testObj).isNotNull();
            cache.release(i);
        }
        for (int i = 0; i < threadNum; i++) {
            assertThat(testObjects[i].isClosed()).isFalse();
        }
        for (int i = 0; i < threadNum; i++) {
            cache.release(i);                       // release put
        }

        // let the test objects expire but not expired
        TestingBasics.waitFor(TIMEOUT_SMALL);
        cache.put(threadNum + 1, new TestObject()); // trigger mark and sweep

        for (int i = 0; i < threadNum; i++) {
            TestObject testObj = cache.get(i);
            assertThat(testObj).isNotNull();        // hasn't expire yet
            cache.release(i);
        }

        // let the test objects expire and be expired
        TestingBasics.waitFor(TIMEOUT_FULL);
        TestObject tmpObj = new TestObject();
        cache.put(threadNum + 1, tmpObj);           // trigger mark and sweep
        for (int i = 0; i < threadNum; i++) {
            assertThat(cache.get(i)).isNull();
        }

        // let the cleaner do the job
        TestingBasics.waitFor(TIMEOUT_SMALL);
        for (int i = 0; i < threadNum; i++) {
            assertThat(testObjects[i].isClosed()).isTrue();
        }

        assertThat(cache.size()).isEqualTo(1);
        assertThat(tmpObj.isClosed()).isFalse();

        cache.removeAll();
        TestingBasics.waitFor(TIMEOUT_SMALL);
        assertThat(tmpObj.isClosed()).isTrue();
        assertThat(cache.size()).isEqualTo(0);
    }

    private record Worker(int id, LRUCache<Integer, TestObject> cache) implements ThreadWorker {
        @Override public void run() {
            TestObject testObj = new TestObject();
            cache.put(id, testObj, 500);

            TestObject testObj2 = cache.get(id);
            assertThat(testObj2).isEqualTo(testObj);
            assertThat(testObj2).isEqualTo(testObj);
            assertThat(testObj2.isClosed()).isFalse();

            cache.release(id);                              // release first put
            cache.release(id);                              // release first get

            TestingBasics.waitFor(TIMEOUT_MEDIUM);
            cache.put(id + 1000, new TestObject(), 500);    // trigger mark&sweep
            assertThat(cache.get(id)).isNull();

            TestingBasics.waitFor(TIMEOUT_SMALL);           // let the cleaner do the job
            assertThat(testObj.isClosed()).isTrue();

            cache.release(id + 1000);
            cache.put(id + 2000, new TestObject());         // trigger mark&sweep

            TestObject testObj_id2000 = cache.remove(id + 2000);
            assertThat(testObj_id2000).isNotNull();
            assertThat(testObj_id2000.isClosed()).isTrue();

            TestingBasics.waitFor(TIMEOUT_SMALL);           // let the cleaner do the job
            assertThat(cache.get(id + 1000)).isNull();
        }
    }

    private record RandomWorker(int idLimit, LRUCache<Integer, TestObject> cache, Random random) implements ThreadWorker {
        @Override public void run() {
            for (int i = 0; i < 10; i++) {
                int id = random.nextInt(idLimit);
                assertThat(cache.get(id)).isNotNull();
                cache.put(id + 1000, new TestObject(), 1000);
                cache.put(id + 2000, new TestObject(), 1000);
                cache.put(id + 3000, new TestObject(), 1000);
                cache.release(id + 1000);
                cache.release(id + 3000);
                cache.release(id);
                TestObject testObj_id2000 = cache.remove(id + 2000);
                if (testObj_id2000 != null) {                // maybe already removed by other threads
                    assertThat(testObj_id2000.isClosed()).isTrue();
                }
            }
        }
    }

    private static class TestObject implements UncheckedClosable {
        private volatile boolean closed = false;

        @Override public void close() {
            closed = true;
        }

        public boolean isClosed() {
            return closed;
        }
    }

    private static @NotNull ExecutorService newExecutor() {
        return CLOSE_ALL.addCloseablePerTest(Executors.newCachedThreadPool());
    }
}
