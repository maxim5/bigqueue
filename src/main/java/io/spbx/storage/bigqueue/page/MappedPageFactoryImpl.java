package io.spbx.storage.bigqueue.page;

import io.spbx.util.base.Unchecked;
import io.spbx.util.io.BasicFiles;
import io.spbx.util.logging.Logger;
import org.jetbrains.annotations.VisibleForTesting;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

/**
 * Mapped page resource manager,
 * responsible for the creation, cache, recycle of the mapped pages.
 * <p>
 * automatic paging and swapping algorithm is leveraged to ensure fast page fetch while
 * keep memory usage efficient at the same time.
 */
public class MappedPageFactoryImpl implements MappedPageFactory {
    private static final Logger log = Logger.forEnclosingClass();

    private static final String PAGE_FILE_NAME = "page";
    private static final String PAGE_FILE_SUFFIX = ".dat";

    private final int pageSize;
    private final Path pageDir;
    private final long ttl;
    private final Map<Long, Object> pageCreationLockMap = new HashMap<>();
    private final LRUCache<Long, MappedPage> cache;

    public MappedPageFactoryImpl(int pageSize, Path pageDir, ExecutorService executor, long cacheTTL) {
        this.pageSize = pageSize;
        this.pageDir = pageDir;
        this.ttl = cacheTTL;
        this.cache = new LRUCacheImpl<>(executor);
        BasicFiles.createDirs(this.pageDir);
    }

    public MappedPageFactoryImpl(int pageSize, Path pageDir, ExecutorService executor) {
        this(pageSize, pageDir, executor, LRUCache.DEFAULT_TTL_MILLIS);
    }

    @Override
    public MappedPage acquirePage(long index) {
        MappedPage mpi = cache.get(index);
        if (mpi == null) { // not in cache, need to create one
            try {
                Object lock;
                synchronized (pageCreationLockMap) {
                    if (!pageCreationLockMap.containsKey(index)) {
                        pageCreationLockMap.put(index, new Object());
                    }
                    lock = pageCreationLockMap.get(index);
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

    /**
     * thread unsafe, caller need synchronization
     */
    @Override
    public void releaseCachedPages() {
        cache.removeAll();
    }

    /**
     * thread unsafe, caller need synchronization
     */
    @Override
    public void deleteAllPages() {
        cache.removeAll();
        Set<Long> indexSet = getExistingBackFileIndexSet();
        deletePages(indexSet);
        log.debug().log("All page files in dir %s have been deleted", pageDir);
    }

    /**
     * thread unsafe, caller need synchronization
     */
    @Override
    public void deletePages(Set<Long> indexes) {
        if (indexes == null) return;
        for (long index : indexes) {
            deletePage(index);
        }
    }

    /**
     * thread unsafe, caller need synchronization
     */
    @Override
    public void deletePage(long index) {
        // remove the page from cache first
        cache.remove(index);
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
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ignore) {}
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
    public Set<Long> getPageIndexSetBefore(long timestamp) {
        Set<Long> beforeIndexSet = new HashSet<>();
        File[] pageFiles = pageDir.toFile().listFiles();
        if (pageFiles != null && pageFiles.length > 0) {
            for (File pageFile : pageFiles) {
                if (pageFile.lastModified() < timestamp) {
                    String fileName = pageFile.getName();
                    if (fileName.endsWith(PAGE_FILE_SUFFIX)) {
                        long index = getIndexByFileName(fileName);
                        beforeIndexSet.add(index);
                    }
                }
            }
        }
        return beforeIndexSet;
    }

    /**
     * thread unsafe, caller need synchronization
     */
    @Override
    public void deletePagesBefore(long timestamp) {
        Set<Long> indexSet = getPageIndexSetBefore(timestamp);
        deletePages(indexSet);
        log.debug().log("All page files in dir [%s], before [%d] have been deleted", pageDir, timestamp);
    }

    @Override
    public void deletePagesBeforePageIndex(long pageIndex) {
        Set<Long> indexSet = getExistingBackFileIndexSet();
        for (Long index : indexSet) {
            if (index < pageIndex) {
                deletePage(index);
            }
        }
    }

    @Override
    public Set<Long> getExistingBackFileIndexSet() {
        Set<Long> indexSet = new HashSet<>();
        File[] pageFiles = pageDir.toFile().listFiles();
        if (pageFiles != null && pageFiles.length > 0) {
            for (File pageFile : pageFiles) {
                String fileName = pageFile.getName();
                if (fileName.endsWith(PAGE_FILE_SUFFIX)) {
                    long index = getIndexByFileName(fileName);
                    indexSet.add(index);
                }
            }
        }
        return indexSet;
    }

    @Override
    public int getCacheSize() {
        return cache.size();
    }

    @VisibleForTesting
    int getLockMapSize() {
        return pageCreationLockMap.size();
    }

    @Override
    public long getPageFileLastModifiedTime(long index) {
        File pageFile = getFileNameByIndex(index).toFile();
        if (!pageFile.exists()) {
            return -1L;
        }
        return pageFile.lastModified();
    }

    @Override
    public long getFirstPageIndexBefore(long timestamp) {
        Set<Long> beforeIndexSet = getPageIndexSetBefore(timestamp);
        if (beforeIndexSet.isEmpty()) return -1L;
        TreeSet<Long> sortedIndexSet = new TreeSet<>(beforeIndexSet);
        Long largestIndex = sortedIndexSet.last();
        return largestIndex;
    }

    /**
     * thread unsafe, caller need synchronization
     */
    @Override
    public void flush() {
        Collection<MappedPage> cachedPages = cache.getValues();
        for (MappedPage mappedPage : cachedPages) {
            mappedPage.flush();
        }
    }

    @Override
    public Set<String> getBackPageFileSet() {
        Set<String> fileSet = new HashSet<>();
        File[] pageFiles = pageDir.toFile().listFiles();
        if (pageFiles != null && pageFiles.length > 0) {
            for (File pageFile : pageFiles) {
                String fileName = pageFile.getName();
                if (fileName.endsWith(PAGE_FILE_SUFFIX)) {
                    fileSet.add(fileName);
                }
            }
        }
        return fileSet;
    }

    @Override
    public long getBackPageFileSize() {
        long totalSize = 0L;
        File[] pageFiles = pageDir.toFile().listFiles();
        if (pageFiles != null && pageFiles.length > 0) {
            for (File pageFile : pageFiles) {
                String fileName = pageFile.getName();
                if (fileName.endsWith(PAGE_FILE_SUFFIX)) {
                    totalSize += pageFile.length();
                }
            }
        }
        return totalSize;
    }

    @VisibleForTesting
    Path getFileNameByIndex(long index) {
        return pageDir.resolve(PAGE_FILE_NAME + "-" + index + PAGE_FILE_SUFFIX);
    }

	private static long getIndexByFileName(String fileName) {
		int beginIndex = fileName.lastIndexOf('-');
		beginIndex += 1;
		int endIndex = fileName.lastIndexOf(PAGE_FILE_SUFFIX);
		String sIndex = fileName.substring(beginIndex, endIndex);
		long index = Long.parseLong(sIndex);
		return index;
	}
}
