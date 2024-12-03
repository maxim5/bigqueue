package io.spbx.storage.bigqueue.page;

import io.spbx.storage.bigqueue.RandomData;
import io.spbx.util.testing.TestingBasics;
import io.spbx.util.testing.concurrent.TestingThreads;
import io.spbx.util.testing.concurrent.ThreadWorker;
import io.spbx.util.testing.concurrent.TraceExecutor;
import io.spbx.util.testing.ext.CloseAllExtension;
import io.spbx.util.testing.ext.TempDirectoryExtension;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static com.google.common.truth.Truth.assertThat;
import static io.spbx.storage.bigqueue.page.Resource.waitForExpire;

@Tag("slow") @Tag("integration")
public class MappedPageFactoryIntegrationTest {
    @RegisterExtension private static final TempDirectoryExtension TEMP_DIRECTORY = TempDirectoryExtension.withCleanup();
    @RegisterExtension private static final CloseAllExtension CLOSE_ALL = new CloseAllExtension();
    private static final RandomData random = RandomData.seededForEachCallSite();
    private static final int TIMEOUT = 100;
    private static final int TTL = 100;

    @Test
    public void getBackPageFileSet_simple() {
        int loop = 10;
        MappedPageFactoryImpl factory = new MappedPageFactoryImpl(1024, currentTempDir(), newExecutor(), TTL);

        for (int i = 0; i < loop; i++) {
            factory.acquirePage(i);
        }
        List<String> files = factory.getBackPageFiles();
        assertThat(files.size()).isEqualTo(loop);
        for (int i = 0; i < loop; i++) {
            assertThat(files.contains(factory.getFileNameByIndex(i).getFileName().toString())).isTrue();
        }
    }

    @Test
    public void getBackPageFileSize_simple() {
        int pageSize = 1024 * 1024;
        int loop = 100;
        MappedPageFactoryImpl factory = new MappedPageFactoryImpl(pageSize, currentTempDir(), newExecutor(), TTL);

        for (int i = 0; i < loop; i++) {
            factory.acquirePage(i);
        }
        assertThat(factory.getBackPageFileSize()).isEqualTo(1024 * 1024 * loop);
    }

    @Test
    public void single_thread() {
        int pageSize = 128 * 1024 * 1024;
        int loop = 100;
        TraceExecutor executor = newExecutor();
        MappedPageFactoryImpl factory = new MappedPageFactoryImpl(pageSize, currentTempDir(), executor, TTL);

        MappedPage mappedPage = factory.acquirePage(0);     // first acquire
        assertThat(mappedPage).isNotNull();
        MappedPage mappedPage0 = factory.acquirePage(0);    // second acquire
        assertThat(mappedPage0).isSameInstanceAs(mappedPage);
        MappedPage mappedPage1 = factory.acquirePage(1);
        assertThat(mappedPage1).isNotSameInstanceAs(mappedPage0);

        factory.releasePage(0);                 // release first acquire
        factory.releasePage(0);                 // release second acquire

        waitForExpire(executor, TTL);           // let page0 expire
        factory.acquirePage(2);                 // trigger mark&sweep and purge old page0
        mappedPage = factory.acquirePage(0);    // create a new page0
        assertThat(mappedPage).isNotSameInstanceAs(mappedPage0);

        waitForExpire(executor, TTL);           // let the async cleaner do the job
        assertThat(mappedPage.isClosed()).isFalse();
        assertThat(mappedPage0.isClosed()).isTrue();

        for (long i = 0; i < loop; i++) {
            assertThat(factory.acquirePage(i)).isNotNull();
        }
        assertThat(factory.getCacheSize()).isEqualTo(loop);
        List<Long> indexes = factory.getExistingBackFileIndexes();
        assertThat(indexes.size()).isEqualTo(loop);
        for (long i = 0; i < loop; i++) {
            assertThat(indexes.contains(i)).isTrue();
        }

        factory.deletePage(0);
        assertThat(factory.getCacheSize()).isEqualTo(loop - 1);
        assertThat(factory.getExistingBackFileIndexes().size()).isEqualTo(loop - 1);

        factory.deletePage(1);
        assertThat(factory.getCacheSize()).isEqualTo(loop - 2);
        assertThat(factory.getExistingBackFileIndexes().size()).isEqualTo(loop - 2);

        for (long i = 2; i < loop / 2; i++) {
            factory.deletePage(i);
        }
        assertThat(factory.getCacheSize()).isEqualTo(loop / 2);
        assertThat(factory.getExistingBackFileIndexes().size()).isEqualTo(loop / 2);

        factory.deleteAllPages();
        assertThat(factory.getCacheSize()).isEqualTo(0);
        assertThat(factory.getExistingBackFileIndexes().size()).isEqualTo(0);
    }

    @Test
    public void single_thread_getPageIndexSetBefore() {
        int pageSize = 128 * 1024 * 1024;
        MappedPageFactoryImpl factory = new MappedPageFactoryImpl(pageSize, currentTempDir(), newExecutor(), TTL);

        long start = System.currentTimeMillis();
        for (long i = 0; i < 5; i++) {
            assertThat(factory.acquirePage(i)).isNotNull();
            TestingBasics.waitFor(TIMEOUT);
        }
        assertThat(factory.getPageIndexesBefore(start - 1000).size()).isEqualTo(0);
        assertThat(factory.getPageIndexesBefore(start + TIMEOUT * 3).size()).isEqualTo(3);
        assertThat(factory.getPageIndexesBefore(start + TIMEOUT * 5).size()).isEqualTo(5);

        factory.deletePagesBefore(start + TIMEOUT * 3);
        assertThat(factory.getExistingBackFileIndexes().size()).isEqualTo(2);
        assertThat(factory.getCacheSize()).isEqualTo(2);

        factory.releaseCachedPages();
        assertThat(factory.getCacheSize()).isEqualTo(0);
        assertThat(factory.getLockMapSize()).isEqualTo(0);
    }

    @Test
    public void single_thread_getPageFileLastModifiedTime() {
        int pageSize = 128 * 1024 * 1024;
        MappedPageFactoryImpl factory = new MappedPageFactoryImpl(pageSize, currentTempDir(), newExecutor(), TTL);

        long start = System.currentTimeMillis();
        for (int i = 0; i <= 100; i++) {
            MappedPage page = factory.acquirePage(i);
            page.getLocalBuffer().put(("hello " + i).getBytes());
            page.setDirty(true);
            page.flush();
            long currentTime = System.currentTimeMillis();
            long iPageFileLastModifiedTime = factory.getPageFileLastModifiedTime(i);
            assertThat(iPageFileLastModifiedTime).isAtLeast(start - 1);
            assertThat(iPageFileLastModifiedTime).isAtMost(currentTime + 1);
            long index = factory.getFirstPageIndexBefore(currentTime + 1);
            assertThat(index).isEqualTo(i);
            start = currentTime;
        }
    }

    @Test
    public void multiple_threads() {
        int pageSize = 128 * 1024 * 1024;
        int pageNum = 200;
        int threadNum = 1000;
        TraceExecutor executor = newExecutor();
        MappedPageFactoryImpl factory = new MappedPageFactoryImpl(pageSize, currentTempDir(), executor, TTL);

        Map<Integer, MappedPage[]> sharedMap1 = runWorkers(factory, threadNum, pageNum);
        assertThat(factory.getCacheSize()).isEqualTo(pageNum);
        Map<Integer, MappedPage[]> sharedMap2 = runWorkers(factory, threadNum, pageNum);
        assertThat(factory.getCacheSize()).isEqualTo(pageNum);
        verifySamePages(sharedMap1, sharedMap2, true);      // pages in two maps should be same since they are all cached
        verifyAllClosed(sharedMap1, false);

        waitForExpire(executor, TTL);
        factory.acquirePage(pageNum + 1);                   // trigger mark&sweep
        assertThat(factory.getCacheSize()).isEqualTo(1);

        Map<Integer, MappedPage[]> sharedMap3 = runWorkers(factory, threadNum, pageNum);
        assertThat(factory.getCacheSize()).isEqualTo(pageNum + 1);

        // pages in two maps should be different since all pages in sharedMap1 has expired and purged out
        verifySamePages(sharedMap1, sharedMap3, false);
        verifyAllClosed(sharedMap3, false);
        verifyAllClosed(sharedMap1, true);

        assertThat(factory.getLockMapSize()).isEqualTo(0);  // ensure no memory leak
    }

    private static Map<Integer, MappedPage[]> runWorkers(MappedPageFactory pageFactory, int threadNum, int pageNum) {
        Map<Integer, MappedPage[]> sharedMap = new ConcurrentHashMap<>();
        for (int i = 0; i < threadNum; i++) {
            MappedPage[] pages = new MappedPage[pageNum];
            sharedMap.put(i, pages);
        }

        CountDownLatch latch = new CountDownLatch(threadNum);
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new Worker(latch, sharedMap.get(i), pageFactory).newThread();
        }
        TestingThreads.startAll(threads);
        TestingThreads.joinAll(threads);

        MappedPage[] firstPageArray = sharedMap.get(0);
        for (MappedPage page : firstPageArray) {
            assertThat(page.isClosed()).isFalse();
        }
        for (int i = 1; i < threadNum; i++) {
            MappedPage[] pageArray = sharedMap.get(i);
            for (int j = 0; j < pageArray.length; j++) {
                assertThat(pageArray[j]).isSameInstanceAs(firstPageArray[j]);
            }
        }

        return sharedMap;
    }

    private static void verifySamePages(Map<Integer, MappedPage[]> map1, Map<Integer, MappedPage[]> map2, boolean same) {
        assertThat(map1.size()).isEqualTo(map2.size());
        for (int i = 0; i < map1.size(); i++) {
            MappedPage[] pages1 = map1.get(i);
            MappedPage[] pages2 = map2.get(i);
            assertThat(pages1.length).isEqualTo(pages2.length);
            for (int j = 0; j < pages1.length; j++) {
                if (same) {
                    assertThat(pages1[j]).isSameInstanceAs(pages2[j]);
                } else {
                    assertThat(pages1[j]).isNotSameInstanceAs(pages2[j]);
                }
            }
        }
    }

    private static void verifyAllClosed(Map<Integer, MappedPage[]> map, boolean closed) {
        map.values().forEach(mappedPages -> {
            for (MappedPage mappedPage : mappedPages) {
                assertThat(mappedPage.isClosed()).isEqualTo(closed);
            }
        });
    }

    private record Worker(CountDownLatch latch, MappedPage[] pages, MappedPageFactory factory) implements ThreadWorker {
        @Override public void run() {
            latch.countDown();
            TestingThreads.runSilently(latch::await);

            List<Long> indexes = random.randomPermutation(pages.length);
            for (long i : indexes) {
                pages[(int) i] = factory.acquirePage(i);
                factory.releasePage(i);
            }
        }
    }

    private static @NotNull Path currentTempDir() {
        return TEMP_DIRECTORY.currentTempDir();
    }

    private static @NotNull TraceExecutor newExecutor() {
        return CLOSE_ALL.addCloseablePerTest(TraceExecutor.wrap(Executors.newCachedThreadPool()));
    }
}
