package io.spbx.storage.bigqueue.page;

import io.spbx.util.testing.TestingThreads;
import io.spbx.util.testing.TestingThreads.ThreadWorker;
import io.spbx.util.testing.ext.CloseAllExtension;
import io.spbx.util.testing.ext.TempDirectoryExtension;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.truth.Truth.assertThat;
import static io.spbx.util.func.ScopeFunctions.also;

@Tag("slow") @Tag("integration")
public class MappedPageIntegrationTest {
    @RegisterExtension private static final TempDirectoryExtension TEMP_DIRECTORY = TempDirectoryExtension.withCleanup();
    @RegisterExtension private static final CloseAllExtension CLOSE_ALL = new CloseAllExtension();
    private static final int TTL = 2 * 1000;

    @Test
    public void single_thread() {
        int pageSize = 1024 * 1024 * 32;
        int loop = 10000;
        MappedPageFactory mappedPageFactory = new MappedPageFactoryImpl(pageSize, currentTempDir(), newExecutor(), TTL);

        MappedPage mappedPage = mappedPageFactory.acquirePage(0);
        assertThat(mappedPage).isNotNull();

        also(mappedPage.getLocalBuffer(), buffer -> {
            assertThat(buffer.limit()).isEqualTo(pageSize);
            assertThat(buffer.position()).isEqualTo(0);
        });

        byte[] data = "hello world".getBytes();
        for (int i = 0; i < loop; i++) {
            mappedPage.getLocalBuffer(i * 20).put(data);
            assertThat(mappedPage.getBufferBytes(i * 20, data.length)).isEqualTo(data);
        }

        ByteBuffer buffer = ByteBuffer.allocateDirect(16);
        buffer.putInt(1);
        buffer.putInt(2);
        buffer.putLong(3L);
        for (int i = 0; i < loop; i++) {
            mappedPage.getLocalBuffer(i * 20).put(buffer.flip());
        }
        for (int i = 0; i < loop; i++) {
            also(mappedPage.getLocalBuffer(i * 20), buf -> {
                assertThat(buf.getInt()).isEqualTo(1);
                assertThat(buf.getInt()).isEqualTo(2);
                assertThat(buf.getLong()).isEqualTo(3L);
            });
        }
    }

    @Test
    public void multiple_threads() {
        int pageSize = 1024 * 1024 * 32;
        int threadNum = 100;
        int pageNumLimit = 50;
        MappedPageFactory mappedPageFactory = new MappedPageFactoryImpl(pageSize, currentTempDir(), newExecutor(), TTL);

        Set<MappedPage> pageSet = Collections.newSetFromMap(new ConcurrentHashMap<>());
        List<ByteBuffer> localBufferList = Collections.synchronizedList(new ArrayList<>());

        Thread[] threads = new Thread[threadNum];
        for (int i = 0; i < threadNum; i++) {
            threads[i] = new Worker(i, mappedPageFactory, pageNumLimit, pageSet, localBufferList).newThread();
        }
        TestingThreads.startAll(threads);
        TestingThreads.joinAll(threads);
        assertThat(threadNum * pageNumLimit).isEqualTo(localBufferList.size());
        assertThat(pageNumLimit).isEqualTo(pageSet.size());

        // Check thread locality
        for (int i = 0; i < localBufferList.size(); i++) {
            for (int j = i + 1; j < localBufferList.size(); j++) {
                assertThat(localBufferList.get(j)).isNotSameInstanceAs(localBufferList.get(i));
            }
        }
    }

    private record Worker(int id,
                          MappedPageFactory factory,
                          int pageNumLimit,
                          Set<MappedPage> sharedPageSet,
                          List<ByteBuffer> localBufferList) implements ThreadWorker {
        @Override public void run() {
            for (int i = 0; i < pageNumLimit; i++) {
                MappedPage page = factory.acquirePage(i);
                sharedPageSet.add(page);
                localBufferList.add(page.getLocalBuffer());

                int startPosition = id * 2048;
                for (int j = 0; j < 100; j++) {
                    byte[] data = ("hello world " + j).getBytes();
                    int length = data.length;
                    page.getLocalBuffer(startPosition + j * 20).put(data);
                    assertThat(page.getBufferBytes(startPosition + j * 20, length)).isEqualTo(data);
                }

                ByteBuffer buffer = ByteBuffer.allocateDirect(16);
                buffer.putInt(1);
                buffer.putInt(2);
                buffer.putLong(3L);
                for (int j = 0; j < 100; j++) {
                    page.getLocalBuffer(startPosition + j * 20).put(buffer.flip());
                }
                for (int j = 0; j < 100; j++) {
                    also(page.getLocalBuffer(startPosition + j * 20), buf -> {
                        assertThat(buf.getInt()).isEqualTo(1);
                        assertThat(buf.getInt()).isEqualTo(2);
                        assertThat(buf.getLong()).isEqualTo(3L);
                    });
                }
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
