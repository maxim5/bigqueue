package io.spbx.storage.bigqueue.page;

import io.spbx.util.base.annotate.CheckReturnValue;
import io.spbx.util.base.annotate.MustBeClosed;
import io.spbx.util.logging.Logger;
import io.spbx.util.testing.extern.mockito.Mocking;
import org.jetbrains.annotations.NotNull;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.ScopedMock;

import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.*;

@ThreadSafe
public class FakeMappedPageFactory implements MappedPageFactory {
    private static final Logger log = Logger.forEnclosingClass();

    private final int pageSize;
    private final Path pageDir;
    private final long ttl;
    private final ConcurrentMap<Long, BackFile> files = new ConcurrentHashMap<>();
    private final LRUCache<Long, MappedPage> cache = new FakeLRUCache<>();

    public FakeMappedPageFactory(int pageSize, @NotNull Path pageDir, long ttl) {
        log.debug().log("FakeMappedPageFactory(%s, %s, %s)", pageSize, pageDir, ttl);
        this.pageSize = pageSize;
        this.pageDir = pageDir;
        this.ttl = ttl;
    }

    public FakeMappedPageFactory(int pageSize, @NotNull Path pageDir) {
        this(pageSize, pageDir, LRUCache.DEFAULT_TTL_MILLIS);
    }

    public static void withMockedFactory(@NotNull Runnable runnable) {
        try (ScopedMock ignore = mockFactory()) {
            runnable.run();
        } finally {
            Mocking.clearMock(MappedPageFactory.class);
        }
    }

    @CheckReturnValue
    @MustBeClosed
    public static @NotNull MockedStatic<MappedPageFactory> mockFactory() {
        MockedStatic<MappedPageFactory> mocked = Mockito.mockStatic(MappedPageFactory.class, Mockito.CALLS_REAL_METHODS);
        mocked.when(() -> MappedPageFactory.newMappedPageFactory(anyInt(), any(), any())).then(invocation -> {
            Integer pageSize = invocation.getArgument(0);
            Path pageDir = invocation.getArgument(1);
            return new FakeMappedPageFactory(pageSize, pageDir);
        });
        mocked.when(() -> MappedPageFactory.newMappedPageFactory(anyInt(), any(), any(), anyLong())).then(invocation -> {
            Integer pageSize = invocation.getArgument(0);
            Path pageDir = invocation.getArgument(1);
            Long ttl = invocation.getArgument(3);
            return new FakeMappedPageFactory(pageSize, pageDir, ttl);
        });
        return mocked;
    }

    @Override
    public MappedPage acquirePage(long index) {
        BackFile backFile = files.computeIfAbsent(index, i -> BackFile.of(getFileNameByIndex(i), ByteBuffer.allocate(pageSize)));
        MappedPage page = cache.get(index);
        if (page == null) {
            page = new FakeMappedPage(backFile.content(), index);
            cache.put(index, page, ttl);
        }
        return page;
    }

    @Override
    public void releasePage(long index) {
        cache.release(index);
    }

    @Override
    public int getPageSize() {
        return pageSize;
    }

    @Override
    public Path getPageDir() {
        return pageDir;
    }

    @Override
    public void deletePage(long index) {
        files.remove(index);
        cache.remove(index);
    }

    @Override
    public void deleteAllPages() {
        files.clear();
        cache.removeAll();
    }

    @Override
    public void deletePagesBefore(long timestamp) {
        List<Long> keys = this.getPageIndexesBefore(timestamp);
        keys.forEach(files::remove);
        keys.forEach(cache::remove);
    }

    @Override
    public void deletePagesBeforePageIndex(long pageIndex) {
        List<Long> keys = files.keySet().stream().filter(key -> key < pageIndex).toList();
        keys.forEach(files::remove);
        keys.forEach(cache::remove);
    }

    @Override
    public void releaseCachedPages() {
        cache.removeAll();
    }

    @Override
    public List<Long> getPageIndexesBefore(long timestamp) {
        return files.entrySet().stream().filter(e -> e.getValue().isBefore(timestamp)).map(Map.Entry::getKey).toList();
    }

    @Override
    public long getPageFileLastModifiedTime(long index) {
        return files.get(index).fileSize();
    }

    @Override
    public long getFirstPageIndexBefore(long timestamp) {
        return getPageIndexesBefore(timestamp).stream().mapToLong(x -> x).max().orElse(-1);
    }

    @Override
    public List<Long> getExistingBackFileIndexes() {
        return new ArrayList<>(files.keySet());
    }

    @Override
    public int getCacheSize() {
        return cache.size();
    }

    @Override
    public List<String> getBackPageFiles() {
        return files.values().stream().map(BackFile::path).map(Path::getFileName).map(Path::toString).toList();
    }

    @Override
    public long getBackPageFileSize() {
        return files.values().stream().mapToLong(BackFile::fileSize).sum();
    }

    @Override
    public void flush() {
        // Do nothing
    }

    private Path getFileNameByIndex(long index) {
        return pageDir.resolve("page-%s.dat".formatted(index));
    }

    private record BackFile(@NotNull Path path, @NotNull AtomicLong lastModified, @NotNull ByteBuffer content) {
        public static BackFile of(@NotNull Path path, @NotNull ByteBuffer content) {
            return new BackFile(path, new AtomicLong(System.currentTimeMillis()), content);
        }

        public long fileSize() {
            return content.capacity();
        }

        public boolean isBefore(long timestamp) {
            return lastModified.get() < timestamp;
        }
    }
}
