package io.spbx.storage.bigqueue.page;

import io.spbx.storage.bigqueue.RandomData;
import io.spbx.util.testing.TestSize;
import io.spbx.util.testing.TestingBasics;
import io.spbx.util.testing.TestingThreads;
import io.spbx.util.testing.TestingThreads.ThreadWorker;
import io.spbx.util.testing.ext.CloseAllExtension;
import io.spbx.util.testing.ext.TempDirectoryExtension;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.truth.Truth.assertThat;

@Tag("slow") @Tag("integration")
public class MappedPageFactoryIntegrationTest {
    @RegisterExtension private static final TempDirectoryExtension TEMP_DIRECTORY = TempDirectoryExtension.withCleanup();
    @RegisterExtension private static final CloseAllExtension CLOSE_ALL = new CloseAllExtension();
    private static final RandomData random = RandomData.seededForEachCallSite();
    private static final int TTL = 2 * 1000;
    private static final int TIMEOUT_SMALL = 600;
    private static final int TIMEOUT_FULL = 2000;

    @Test
    public void getBackPageFileSet_simple() {
        int loop = 10;
        MappedPageFactoryImpl factory = new MappedPageFactoryImpl(1024, currentTempDir(), newExecutor(), TTL);

        for (int i = 0; i < loop; i++) {
            factory.acquirePage(i);
        }
        Set<String> fileSet = factory.getBackPageFileSet();
        assertThat(fileSet.size()).isEqualTo(loop);
        for (int i = 0; i < loop; i++) {
            assertThat(fileSet.contains(factory.getFileNameByIndex(i).getFileName().toString())).isTrue();
        }
    }

    @Test
    public void getBackPageFileSize_simple() {
        int pageSize = 1024 * 1024;
        int loop = 100;
        MappedPageFactory factory = new MappedPageFactoryImpl(pageSize, currentTempDir(), newExecutor(), TTL);

        for (int i = 0; i < loop; i++) {
            factory.acquirePage(i);
        }
        assertThat(factory.getBackPageFileSize()).isEqualTo(1024 * 1024 * loop);
    }

    @Test
    public void single_thread() {
        TestSize.assumeTestSize(TestSize.STRESS);
        int pageSize = 128 * 1024 * 1024;
        MappedPageFactoryImpl factory = new MappedPageFactoryImpl(pageSize, currentTempDir(), newExecutor(), TTL);

        MappedPage mappedPage = factory.acquirePage(0); // first acquire
        assertThat(mappedPage).isNotNull();
        MappedPage mappedPage0 = factory.acquirePage(0); // second acquire
        assertThat(mappedPage0).isSameInstanceAs(mappedPage);
        MappedPage mappedPage1 = factory.acquirePage(1);
        assertThat(mappedPage1).isNotSameInstanceAs(mappedPage0);

        factory.releasePage(0);                 // release first acquire
        factory.releasePage(0);                 // release second acquire

        TestingBasics.waitFor(TIMEOUT_FULL);    // let page0 expire
        factory.acquirePage(2);                 // trigger mark&sweep and purge old page0
        mappedPage = factory.acquirePage(0);    // create a new page0
        assertThat(mappedPage).isNotSameInstanceAs(mappedPage0);

        TestingBasics.waitFor(TIMEOUT_SMALL);   // let the async cleaner do the job
        assertThat(mappedPage.isClosed()).isFalse();
        assertThat(mappedPage0.isClosed()).isTrue();

        for (long i = 0; i < 100; i++) {
            assertThat(factory.acquirePage(i)).isNotNull();
        }
        assertThat(factory.getCacheSize()).isEqualTo(100);
        Set<Long> indexSet = factory.getExistingBackFileIndexSet();
        assertThat(indexSet.size()).isEqualTo(100);
        for (long i = 0; i < 100; i++) {
            assertThat(indexSet.contains(i)).isTrue();
        }

        factory.deletePage(0);
        assertThat(factory.getCacheSize()).isEqualTo(99);
        assertThat(factory.getExistingBackFileIndexSet().size()).isEqualTo(99);

        factory.deletePage(1);
        assertThat(factory.getCacheSize()).isEqualTo(98);
        assertThat(factory.getExistingBackFileIndexSet().size()).isEqualTo(98);

        for (long i = 2; i < 50; i++) {
            factory.deletePage(i);
        }
        assertThat(factory.getCacheSize()).isEqualTo(50);
        assertThat(factory.getExistingBackFileIndexSet().size()).isEqualTo(50);

        factory.deleteAllPages();
        assertThat(factory.getCacheSize()).isEqualTo(0);
        assertThat(factory.getExistingBackFileIndexSet().size()).isEqualTo(0);

        long start = System.currentTimeMillis();
        for (long i = 0; i < 5; i++) {
            assertThat(factory.acquirePage(i)).isNotNull();
            TestingBasics.waitFor(TIMEOUT_SMALL);
        }
        assertThat(factory.getPageIndexSetBefore(start - 1000).size()).isEqualTo(0);
        assertThat(factory.getPageIndexSetBefore(start + TIMEOUT_SMALL * 3).size()).isEqualTo(3);
        assertThat(factory.getPageIndexSetBefore(start + TIMEOUT_SMALL * 5).size()).isEqualTo(5);

        factory.deletePagesBefore(start + TIMEOUT_SMALL * 3);
        assertThat(factory.getExistingBackFileIndexSet().size()).isEqualTo(2);
        assertThat(factory.getCacheSize()).isEqualTo(2);

        factory.releaseCachedPages();
        assertThat(factory.getCacheSize()).isEqualTo(0);

        assertThat(factory.getLockMapSize()).isEqualTo(0);
        factory.deleteAllPages();

        start = System.currentTimeMillis();
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
        MappedPageFactoryImpl factory = new MappedPageFactoryImpl(pageSize, currentTempDir(), newExecutor(), TTL);

        Map<Integer, MappedPage[]> sharedMap1 = this.testAndGetSharedMap(factory, threadNum, pageNum);
        assertThat(pageNum).isEqualTo(factory.getCacheSize());
        Map<Integer, MappedPage[]> sharedMap2 = this.testAndGetSharedMap(factory, threadNum, pageNum);
        assertThat(pageNum).isEqualTo(factory.getCacheSize());
        // pages in two maps should be same since they are all cached
        verifyMap(sharedMap1, sharedMap2, threadNum, pageNum, true);
        verifyClosed(sharedMap1, threadNum, pageNum, false);

        TestingBasics.waitFor(TIMEOUT_FULL);
        factory.acquirePage(pageNum + 1); // trigger mark&sweep
        assertThat(factory.getCacheSize()).isEqualTo(1);
        Map<Integer, MappedPage[]> sharedMap3 = this.testAndGetSharedMap(factory, threadNum, pageNum);
        assertThat(pageNum + 1).isEqualTo(factory.getCacheSize());
        // pages in two maps should be different since all pages in sharedMap1 has expired and purged out
        verifyMap(sharedMap1, sharedMap3, threadNum, pageNum, false);
        verifyClosed(sharedMap3, threadNum, pageNum, false);
        verifyClosed(sharedMap1, threadNum, pageNum, true);

        // ensure no memory leak
        assertThat(factory.getLockMapSize()).isEqualTo(0);
    }

    private void verifyClosed(Map<Integer, MappedPage[]> map, int threadNum, int pageNum, boolean closed) {
        for (int i = 0; i < threadNum; i++) {
            MappedPage[] pageArray = map.get(i);
            for (int j = 0; j < pageNum; j++) {
                assertThat(pageArray[j].isClosed()).isEqualTo(closed);
            }
        }
    }

    private void verifyMap(Map<Integer, MappedPage[]> map1, Map<Integer, MappedPage[]> map2, int threadNum, int pageNum, boolean same) {
        for (int i = 0; i < threadNum; i++) {
            MappedPage[] pageArray1 = map1.get(i);
            MappedPage[] pageArray2 = map2.get(i);
            for (int j = 0; j < pageNum; j++) {
                if (same) {
                    assertThat(pageArray1[j]).isSameInstanceAs(pageArray2[j]);
                } else {
                    assertThat(pageArray1[j]).isNotSameInstanceAs(pageArray2[j]);
                }
            }
        }
    }

    private Map<Integer, MappedPage[]> testAndGetSharedMap(MappedPageFactory pageFactory, int threadNum, int pageNum) {
        // init shared map
        Map<Integer, MappedPage[]> sharedMap = new ConcurrentHashMap<>();
        for (int i = 0; i < threadNum; i++) {
            MappedPage[] pageArray = new MappedPage[pageNum];
            sharedMap.put(i, pageArray);
        }

        // Run workers
        CountDownLatch latch = new CountDownLatch(threadNum);
        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new Worker(latch, sharedMap.get(i), pageFactory, pageNum).newThread();
        }
        TestingThreads.startAll(threads);
        TestingThreads.joinAll(threads);

        // validate
        MappedPage[] firstPageArray = sharedMap.get(0);
        for (int j = 0; j < pageNum; j++) {
            MappedPage page = firstPageArray[j];
            assertThat(page.isClosed()).isFalse();
        }
        for (int i = 1; i < threadNum; i++) {
            MappedPage[] pageArray = sharedMap.get(i);
            for (int j = 0; j < pageNum; j++) {
                assertThat(pageArray[j]).isSameInstanceAs(firstPageArray[j]);
            }
        }

        return sharedMap;
    }

    private record Worker(CountDownLatch latch, MappedPage[] pages, MappedPageFactory factory, int pageNum) implements ThreadWorker {
        @Override public void run() {
            latch.countDown();
            TestingThreads.runSilently(latch::await);

            List<Long> indexes = random.randomPermutation(pageNum);
            for (long i : indexes) {
                pages[(int) i] = factory.acquirePage(i);
                factory.releasePage(i);
            }
        }
    }

    private static @NotNull Path currentTempDir() {
        return TEMP_DIRECTORY.currentTempDir();
    }

    private static @NotNull ExecutorService newExecutor() {
        return CLOSE_ALL.addCloseablePerTest(Executors.newCachedThreadPool());
    }
}
