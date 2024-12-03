package io.spbx.storage.bigqueue.page;

import io.spbx.util.base.error.Unchecked;
import io.spbx.util.func.LongScopeFunctions;
import io.spbx.util.io.BasicFiles;
import io.spbx.util.logging.Logger;
import org.jetbrains.annotations.VisibleForTesting;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static io.spbx.util.collect.stream.BasicStreams.empty;
import static io.spbx.util.collect.stream.BasicStreams.streamOf;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * Mapped page resource manager,
 * responsible for the creation, cache, recycle of the mapped pages.
 * <p>
 * automatic paging and swapping algorithm is leveraged to ensure fast page fetch while
 * keep memory usage efficient at the same time.
 */
@ThreadSafe
class MappedPageFactoryImpl implements MappedPageFactory {
    private static final Logger log = Logger.forEnclosingClass();

    private static final String PAGE_FILE_NAME = "page";
    private static final String PAGE_FILE_SUFFIX = ".dat";

    private final int pageSize;
    private final Path pageDir;
    private final long ttl;
    private final Map<Long, Object> pageCreationLockMap = new HashMap<>();
    private final LRUCache<Long, MappedPage> cache;

    MappedPageFactoryImpl(int pageSize, Path pageDir, ExecutorService executor, long cacheTTL) {
        this.pageSize = pageSize;
        this.pageDir = pageDir;
        this.ttl = cacheTTL;
        this.cache = new LRUCacheImpl<>(executor);
        BasicFiles.createDirs(this.pageDir);
    }

    MappedPageFactoryImpl(int pageSize, Path pageDir, ExecutorService executor) {
        this(pageSize, pageDir, executor, LRUCache.DEFAULT_TTL_MILLIS);
    }

    @Override
    public MappedPage acquirePage(long index) {
        MappedPage mpi = cache.get(index);
        if (mpi == null) { // not in cache, need to create one
            try {
                Object lock;
                synchronized (pageCreationLockMap) {
                    lock = pageCreationLockMap.computeIfAbsent(index, k -> new Object());
                }
                synchronized (lock) { // only lock the creation of page index
                    mpi = cache.get(index); // double check
                    if (mpi == null) {
                        Path fileName = getFileNameByIndex(index);
                        try (RandomAccessFile randomAccessFile = new RandomAccessFile(fileName.toFile(), "rw");
                             FileChannel channel = randomAccessFile.getChannel()) {
                            MappedByteBuffer mbb = channel.map(READ_WRITE, 0, pageSize);
                            mpi = new MappedPageImpl(mbb, fileName, index);
                            cache.put(index, mpi, ttl);
                            log.debug().log("Mapped page for %s was just created and cached", fileName);
                        } catch (IOException e) {
                            Unchecked.rethrow(e);
                        }
                    }
                }
            } finally {
                synchronized (pageCreationLockMap) {
                    pageCreationLockMap.remove(index);
                }
            }
        } else {
            log.trace().log("Hit mapped page %s in cache", mpi.getPageFile());
        }
        return mpi;
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
    public void releasePage(long index) {
        cache.release(index);
    }

    @Override
    public void releaseCachedPages() {
        cache.removeAll();
    }

    @Override
    public void deleteAllPages() {
        cache.removeAll();
        List<Long> indexes = getExistingBackFileIndexes();
        indexes.forEach(this::deletePage);
        log.debug().log("All page files in dir %s have been deleted", pageDir);
    }

    @Override
    public void deletePage(long index) {
        cache.remove(index);                            // remove the page from cache first
        Path fileName = getFileNameByIndex(index);
        int count = 0;
        int maxRound = 10;
        boolean deleted = false;
        while (count < maxRound) {
            try {
                BasicFiles.deleteIfExists(fileName);
                deleted = true;
                break;
            } catch (IllegalStateException e) {
                tryToWait();
                count++;
                log.warn().withCause(e).log("Failed to delete file %s, tried round = %d", fileName, count);
            }
        }
        if (deleted) {
            log.info().log("Page file %s was just deleted", fileName);
        } else {
            log.warn().log("Failed to delete file %s after max %d rounds of try, you may delete it manually",
                           fileName, maxRound);
        }
    }

    @Override
    public void deletePagesBefore(long timestamp) {
        List<Long> indexes = getPageIndexesBefore(timestamp);
        indexes.forEach(this::deletePage);
        log.debug().log("All page files in dir [%s], before [%d] have been deleted", pageDir, timestamp);
    }

    @Override
    public void deletePagesBeforePageIndex(long pageIndex) {
        getExistingBackFileIndexes().stream().filter(index -> index < pageIndex).forEach(this::deletePage);
    }

    @Override
    public long getPageFileLastModifiedTime(long index) {
        return LongScopeFunctions.with(getFileNameByIndex(index).toFile(), file -> file.exists() ? file.lastModified() : -1L);
    }

    @Override
    public List<Long> getPageIndexesBefore(long timestamp) {
        return listPageFiles().filter(file -> file.lastModified() < timestamp).map(File::getName).map(this::getIndexByFileName).toList();
    }

    @Override
    public List<String> getBackPageFiles() {
        return listPageFiles().map(File::getName).toList();
    }

    @Override
    public List<Long> getExistingBackFileIndexes() {
        return listPageFiles().map(File::getName).map(this::getIndexByFileName).toList();
    }

    @Override
    public long getBackPageFileSize() {
        return listPageFiles().mapToLong(File::length).sum();
    }

    @Override
    public int getCacheSize() {
        return cache.size();
    }

    @Override
    public void flush() {
        cache.forEachValue(MappedPage::flush);
    }

    @VisibleForTesting
    int getLockMapSize() {
        return pageCreationLockMap.size();
    }

    private Stream<File> listPageFiles() {
        File[] files = pageDir.toFile().listFiles();
        if (files == null) {
            log.warn().log("Failed to list % directory", pageDir);
            return empty();
        }
        return streamOf(files).filter(file -> file.getName().endsWith(PAGE_FILE_SUFFIX));
    }

    @VisibleForTesting
    Path getFileNameByIndex(long index) {
        return pageDir.resolve(PAGE_FILE_NAME + "-" + index + PAGE_FILE_SUFFIX);
    }

	private long getIndexByFileName(String fileName) {
		int beginIndex = fileName.lastIndexOf('-');
		int endIndex = fileName.lastIndexOf(PAGE_FILE_SUFFIX);
        return Long.parseLong(fileName.substring(beginIndex + 1, endIndex));
	}

    private static void tryToWait() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException ignore) {}
    }
}
