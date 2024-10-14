package io.spbx.storage.bigqueue;

import io.spbx.storage.bigqueue.page.MappedPage;
import io.spbx.storage.bigqueue.page.MappedPageFactory;
import io.spbx.storage.bigqueue.page.MappedPageFactoryImpl;
import org.jetbrains.annotations.VisibleForTesting;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.spbx.util.base.BasicExceptions.newIllegalArgumentException;
import static io.spbx.util.base.Unchecked.Suppliers.runRethrow;

/**
 * A big array implementation supporting sequential append and random read.
 * <p>
 * Main features:
 * 1. FAST : close to the speed of direct memory access, extremely fast in append only and sequential read modes,
 * sequential append and read are close to O(1) memory access, random read is close to O(1) memory access if
 * data is in cache and is close to O(1) disk access if data is not in cache.
 * 2. MEMORY-EFFICIENT : automatic paging & swapping algorithm, only most-recently accessed data is kept in memory.
 * 3. THREAD-SAFE : multiple threads can concurrently read/append the array without data corruption.
 * 4. PERSISTENT - all array data is persisted on disk, and is crash resistant.
 * 5. BIG(HUGE) - the total size of the array data is only limited by the available disk space.
 */
public class BigArrayImpl implements BigArray {
    // folder name for index page
    private static final String INDEX_PAGE_DIR = "index";
    // folder name for data page
    private static final String DATA_PAGE_DIR = "data";
    // folder name for meta data page
    private static final String META_DATA_PAGE_DIR = "meta_data";

    // 2 ^ 17 = 1024 * 128
    private static final int INDEX_ITEMS_PER_PAGE_BITS = 17; // 1024 * 128
    // number of items per page
    private static final int INDEX_ITEMS_PER_PAGE = 1 << INDEX_ITEMS_PER_PAGE_BITS;
    // 2 ^ 5 = 32
    private static final int INDEX_ITEM_LENGTH_BITS = 5;
    // length in bytes of an index item
    private static final int INDEX_ITEM_LENGTH = 1 << INDEX_ITEM_LENGTH_BITS;
    // size in bytes of an index page
    /* package */ static final int INDEX_PAGE_SIZE = INDEX_ITEM_LENGTH * INDEX_ITEMS_PER_PAGE;

    // size in bytes of a data page
    private final int DATA_PAGE_SIZE;

    // default size in bytes of a data page
    public static final int DEFAULT_DATA_PAGE_SIZE = 128 * 1024 * 1024;
    // minimum size in bytes of a data page
    public static final int MINIMUM_DATA_PAGE_SIZE = 32 * 1024 * 1024;
    // seconds, time to live for index page cached in memory
    private static final int INDEX_PAGE_CACHE_TTL = 1000;
    // seconds, time to live for data page cached in memory
    private static final int DATA_PAGE_CACHE_TTL = 1000;
    // 2 ^ 4 = 16
    private static final int META_DATA_ITEM_LENGTH_BITS = 4;
    // size in bytes of a meta data page
    private static final int META_DATA_PAGE_SIZE = 1 << META_DATA_ITEM_LENGTH_BITS;

    //	private static final int INDEX_ITEM_DATA_PAGE_INDEX_OFFSET = 0;
    //	private static final int INDEX_ITEM_DATA_ITEM_OFFSET_OFFSET = 8;
    private static final int INDEX_ITEM_DATA_ITEM_LENGTH_OFFSET = 12;
    // timestamp offset of a data item within an index item
    private static final int INDEX_ITEM_DATA_ITEM_TIMESTAMP_OFFSET = 16;

    // directory to persist array data
    private final Path arrayDir;

    private final ExecutorService executor = Executors.newCachedThreadPool();

    // factory for index page management(acquire, release, cache)
    private MappedPageFactory indexPageFactory;
    // factory for data page management(acquire, release, cache)
    private MappedPageFactory dataPageFactory;
    // factory for meta data page management(acquire, release, cache)
    private MappedPageFactory metaPageFactory;

    // only use the first page
    private static final long META_DATA_PAGE_INDEX = 0;

    // head index of the big array, this is the read write barrier.
    // readers can only read items before this index, and writes can write this index or after
    /* package */ final AtomicLong arrayHeadIndex = new AtomicLong();
    // tail index of the big array,
    // readers can't read items before this tail
    /* package */ final AtomicLong arrayTailIndex = new AtomicLong();

    // head index of the data page, this is the to be appended data page index
    private long headDataPageIndex;
    // head offset of the data page, this is the to be appended data offset
    private int headDataItemOffset;

    // lock for appending state management
    private final Lock appendLock = new ReentrantLock();
    // global lock for array read and write management
    private final ReadWriteLock arrayLock = new ReentrantReadWriteLock();

    /**
     * A big array implementation supporting sequential write and random read,
     * use default back data file size per page, see {@link #DEFAULT_DATA_PAGE_SIZE}.
     *
     * @param arrayDir  directory for array data store
     * @param arrayName the name of the array, will be appended as last part of the array directory
     */
    public BigArrayImpl(Path arrayDir, String arrayName) {
        this(arrayDir, arrayName, DEFAULT_DATA_PAGE_SIZE);
    }

    /**
     * A big array implementation supporting sequential write and random read.
     *
     * @param arrayDir  directory for array data store
     * @param arrayName the name of the array, will be appended as last part of the array directory
     * @param pageSize  the back data file size per page in bytes, see minimum allowed {@link #MINIMUM_DATA_PAGE_SIZE}.
     */
    public BigArrayImpl(Path arrayDir, String arrayName, int pageSize) {
        // append array name as part of the directory
        this.arrayDir = runRethrow(() -> arrayDir.resolve(arrayName).normalize().toFile().getCanonicalFile().toPath());

        if (pageSize < MINIMUM_DATA_PAGE_SIZE) {
            throw newIllegalArgumentException("Invalid page size, allowed minimum is %d bytes", MINIMUM_DATA_PAGE_SIZE);
        }

        DATA_PAGE_SIZE = pageSize;
        commonInit();
    }

    private void commonInit() {
        // initialize page factories
        indexPageFactory = new MappedPageFactoryImpl(INDEX_PAGE_SIZE, arrayDir.resolve(INDEX_PAGE_DIR), executor, INDEX_PAGE_CACHE_TTL);
        dataPageFactory = new MappedPageFactoryImpl(DATA_PAGE_SIZE, arrayDir.resolve(DATA_PAGE_DIR), executor, DATA_PAGE_CACHE_TTL);
        // the ttl does not matter here since metadata page is always cached
        metaPageFactory = new MappedPageFactoryImpl(META_DATA_PAGE_SIZE, arrayDir.resolve(META_DATA_PAGE_DIR), executor);

        // initialize array indexes
        initArrayIndex();

        // initialize data page indexes
        initDataPageIndex();
    }

    Path arrayDirectory() {
        return arrayDir;
    }

    ExecutorService executor() {
        return executor;
    }

    Lock arrayWriteLock() {
        return arrayLock.writeLock();
    }

    Lock arrayReadLock() {
        return arrayLock.readLock();
    }

    @Override
    public void removeAll() {
        try {
            arrayWriteLock().lock();
            indexPageFactory.deleteAllPages();
            dataPageFactory.deleteAllPages();
            metaPageFactory.deleteAllPages();
            // FileUtil.deleteDirectory(new File(arrayDirectory));
            commonInit();
        } finally {
            arrayWriteLock().unlock();
        }
    }

    @Override
    public void removeBeforeIndex(long index) {
        try {
            arrayWriteLock().lock();

            validateIndex(index);

            long indexPageIndex = div(index, INDEX_ITEMS_PER_PAGE_BITS);

            ByteBuffer indexItemBuffer = getIndexItemBuffer(index);
            long dataPageIndex = indexItemBuffer.getLong();

            if (indexPageIndex > 0L) {
                indexPageFactory.deletePagesBeforePageIndex(indexPageIndex);
            }
            if (dataPageIndex > 0L) {
                dataPageFactory.deletePagesBeforePageIndex(dataPageIndex);
            }

            // advance the tail to index
            arrayTailIndex.set(index);
        } finally {
            arrayWriteLock().unlock();
        }
    }

    @Override
    public void removeBefore(long timestamp) {
        try {
            arrayWriteLock().lock();
            long firstIndexPageIndex = indexPageFactory.getFirstPageIndexBefore(timestamp);
            if (firstIndexPageIndex >= 0) {
                //				long nextIndexPageIndex = firstIndexPageIndex;
                //				if (nextIndexPageIndex == Long.MAX_VALUE) { //wrap
                //					nextIndexPageIndex = 0L;
                //				} else {
                //					nextIndexPageIndex++;
                //				}
                long toRemoveBeforeIndex = mul(firstIndexPageIndex, INDEX_ITEMS_PER_PAGE_BITS);
                removeBeforeIndex(toRemoveBeforeIndex);
            }
        } catch (IndexOutOfBoundsException e) {
            // ignore
        } finally {
            arrayWriteLock().unlock();
        }
    }

    // find out array head/tail from the meta data
    void initArrayIndex() {
        MappedPage metaDataPage = metaPageFactory.acquirePage(META_DATA_PAGE_INDEX);
        ByteBuffer metaBuf = metaDataPage.getLocalBuffer();
        long head = metaBuf.getLong();
        long tail = metaBuf.getLong();

        arrayHeadIndex.set(head);
        arrayTailIndex.set(tail);
    }

    // find out data page head index and offset
    void initDataPageIndex() {
        if (isEmpty()) {
            headDataPageIndex = 0L;
            headDataItemOffset = 0;
        } else {
            MappedPage previousIndexPage = null;
            long previousIndexPageIndex = -1;
            try {
                long previousIndex = arrayHeadIndex.get() - 1;
                previousIndexPageIndex = div(previousIndex, INDEX_ITEMS_PER_PAGE_BITS); // shift optimization
                previousIndexPage = indexPageFactory.acquirePage(previousIndexPageIndex);
                int previousIndexPageOffset = (int) (mul(mod(previousIndex, INDEX_ITEMS_PER_PAGE_BITS), INDEX_ITEM_LENGTH_BITS));
                ByteBuffer previousIndexItemBuffer = previousIndexPage.getLocalBuffer(previousIndexPageOffset);
                long previousDataPageIndex = previousIndexItemBuffer.getLong();
                int previousDataItemOffset = previousIndexItemBuffer.getInt();
                int perviousDataItemLength = previousIndexItemBuffer.getInt();

                headDataPageIndex = previousDataPageIndex;
                headDataItemOffset = previousDataItemOffset + perviousDataItemLength;
            } finally {
                if (previousIndexPage != null) {
                    indexPageFactory.releasePage(previousIndexPageIndex);
                }
            }
        }
    }

    /**
     * Append the data into the head of the array
     */
    public long append(byte[] data) {
        try {
            arrayReadLock().lock();
            MappedPage toAppendDataPage = null;
            MappedPage toAppendIndexPage = null;
            long toAppendIndexPageIndex = -1L;
            long toAppendDataPageIndex = -1L;
            long toAppendArrayIndex = -1L;

            try {
                appendLock.lock(); // only one thread can append

                // prepare the data pointer
                if (headDataItemOffset + data.length > DATA_PAGE_SIZE) { // not enough space
                    headDataPageIndex++;
                    headDataItemOffset = 0;
                }

                toAppendDataPageIndex = headDataPageIndex;
                int toAppendDataItemOffset = headDataItemOffset;

                toAppendArrayIndex = arrayHeadIndex.get();

                // append data
                toAppendDataPage = dataPageFactory.acquirePage(toAppendDataPageIndex);
                ByteBuffer toAppendDataPageBuffer = toAppendDataPage.getLocalBuffer(toAppendDataItemOffset);
                toAppendDataPageBuffer.put(data);
                toAppendDataPage.setDirty(true);
                // update to next
                headDataItemOffset += data.length;

                toAppendIndexPageIndex = div(toAppendArrayIndex, INDEX_ITEMS_PER_PAGE_BITS); // shift optimization
                toAppendIndexPage = indexPageFactory.acquirePage(toAppendIndexPageIndex);
                int toAppendIndexItemOffset = (int) (mul(mod(toAppendArrayIndex, INDEX_ITEMS_PER_PAGE_BITS), INDEX_ITEM_LENGTH_BITS));

                // update index
                ByteBuffer toAppendIndexPageBuffer = toAppendIndexPage.getLocalBuffer(toAppendIndexItemOffset);
                toAppendIndexPageBuffer.putLong(toAppendDataPageIndex);
                toAppendIndexPageBuffer.putInt(toAppendDataItemOffset);
                toAppendIndexPageBuffer.putInt(data.length);
                long currentTime = System.currentTimeMillis();
                toAppendIndexPageBuffer.putLong(currentTime);
                toAppendIndexPage.setDirty(true);

                // advance the head
                arrayHeadIndex.incrementAndGet();

                // update meta data
                MappedPage metaDataPage = metaPageFactory.acquirePage(META_DATA_PAGE_INDEX);
                ByteBuffer metaDataBuf = metaDataPage.getLocalBuffer();
                metaDataBuf.putLong(arrayHeadIndex.get());
                metaDataBuf.putLong(arrayTailIndex.get());
                metaDataPage.setDirty(true);
            } finally {
                appendLock.unlock();

                if (toAppendDataPage != null) {
                    dataPageFactory.releasePage(toAppendDataPageIndex);
                }
                if (toAppendIndexPage != null) {
                    indexPageFactory.releasePage(toAppendIndexPageIndex);
                }
            }

            return toAppendArrayIndex;
        } finally {
            arrayReadLock().unlock();
        }
    }

    @Override
    public void flush() {
        try {
            arrayReadLock().lock();
            //			try {
            //				appendLock.lock(); // make flush and append mutually exclusive

            metaPageFactory.flush();
            indexPageFactory.flush();
            dataPageFactory.flush();

            //			} finally {
            //				appendLock.unlock();
            //			}
        } finally {
            arrayReadLock().unlock();
        }

    }

    public byte[] get(long index) {
        try {
            arrayReadLock().lock();
            validateIndex(index);

            MappedPage dataPage = null;
            long dataPageIndex = -1L;
            try {
                ByteBuffer indexItemBuffer = getIndexItemBuffer(index);
                dataPageIndex = indexItemBuffer.getLong();
                int dataItemOffset = indexItemBuffer.getInt();
                int dataItemLength = indexItemBuffer.getInt();
                dataPage = dataPageFactory.acquirePage(dataPageIndex);
                byte[] data = dataPage.getBufferBytes(dataItemOffset, dataItemLength);
                return data;
            } finally {
                if (dataPage != null) {
                    dataPageFactory.releasePage(dataPageIndex);
                }
            }
        } finally {
            arrayReadLock().unlock();
        }
    }

    public long getTimestamp(long index) {
        try {
            arrayReadLock().lock();
            validateIndex(index);

            ByteBuffer indexItemBuffer = getIndexItemBuffer(index);
            // position to the timestamp
            int position = indexItemBuffer.position();
            indexItemBuffer.position(position + INDEX_ITEM_DATA_ITEM_TIMESTAMP_OFFSET);
            long ts = indexItemBuffer.getLong();
            return ts;
        } finally {
            arrayReadLock().unlock();
        }
    }

    ByteBuffer getIndexItemBuffer(long index) {
        MappedPage indexPage = null;
        long indexPageIndex = -1L;
        try {
            indexPageIndex = div(index, INDEX_ITEMS_PER_PAGE_BITS); // shift optimization
            indexPage = indexPageFactory.acquirePage(indexPageIndex);
            int indexItemOffset = (int) (mul(mod(index, INDEX_ITEMS_PER_PAGE_BITS), INDEX_ITEM_LENGTH_BITS));

            ByteBuffer indexItemBuffer = indexPage.getLocalBuffer(indexItemOffset);
            return indexItemBuffer;
        } finally {
            if (indexPage != null) {
                indexPageFactory.releasePage(indexPageIndex);
            }
        }
    }

    void validateIndex(long index) {
        if (arrayTailIndex.get() <= arrayHeadIndex.get()) {
            if (index < arrayTailIndex.get() || index >= arrayHeadIndex.get()) {
                throw new IndexOutOfBoundsException();
            }
        } else {
            if (index < arrayTailIndex.get() && index >= arrayHeadIndex.get()) {
                throw new IndexOutOfBoundsException();
            }
        }
    }

    @Override
    public long size() {
        try {
            arrayReadLock().lock();
            return arrayHeadIndex.get() - arrayTailIndex.get();
        } finally {
            arrayReadLock().unlock();
        }
    }

    @Override
    public long getHeadIndex() {
        try {
            arrayReadLock().lock();
            return arrayHeadIndex.get();
        } finally {
            arrayReadLock().unlock();
        }
    }

    @Override
    public long getTailIndex() {
        try {
            arrayReadLock().lock();
            return arrayTailIndex.get();
        } finally {
            arrayReadLock().unlock();
        }
    }

    @Override
    public void close() {
        try {
            arrayWriteLock().lock();
            if (metaPageFactory != null) {
                metaPageFactory.releaseCachedPages();
            }
            if (indexPageFactory != null) {
                indexPageFactory.releaseCachedPages();
            }
            if (dataPageFactory != null) {
                dataPageFactory.releaseCachedPages();
            }
            executor.close();
        } finally {
            arrayWriteLock().unlock();
        }
    }

    @Override
    public int getDataPageSize() {
        return DATA_PAGE_SIZE;
    }

    @Override
    public long findClosestIndex(long timestamp) {
        try {
            arrayReadLock().lock();
            long closestIndex = -1;
            long tailIndex = arrayTailIndex.get();
            long headIndex = arrayHeadIndex.get();
            if (tailIndex == headIndex) return closestIndex; // empty
            long lastIndex = headIndex - 1;
            if (lastIndex < 0) {
                lastIndex = Long.MAX_VALUE;
            }
            if (tailIndex < lastIndex) {
                closestIndex = closestBinarySearch(tailIndex, lastIndex, timestamp);
            } else {
                long lowPartClosestIndex = closestBinarySearch(0L, lastIndex, timestamp);
                long highPartClosetIndex = closestBinarySearch(tailIndex, Long.MAX_VALUE, timestamp);

                long lowPartTimestamp = getTimestamp(lowPartClosestIndex);
                long highPartTimestamp = getTimestamp(highPartClosetIndex);

                closestIndex = Math.abs(timestamp - lowPartTimestamp) < Math.abs(timestamp - highPartTimestamp)
                    ? lowPartClosestIndex : highPartClosetIndex;
            }

            return closestIndex;
        } finally {
            arrayReadLock().unlock();
        }
    }

    private long closestBinarySearch(long low, long high, long timestamp) {
        long mid;
        long sum = low + high;
        if (sum < 0) { // overflow
            BigInteger bigSum = BigInteger.valueOf(low);
            bigSum = bigSum.add(BigInteger.valueOf(high));
            mid = bigSum.shiftRight(1).longValue();
        } else {
            mid = sum / 2;
        }

        long midTimestamp = getTimestamp(mid);

        if (midTimestamp < timestamp) {
            long nextLow = mid + 1;
            if (nextLow >= high) {
                return high;
            }
            return closestBinarySearch(nextLow, high, timestamp);
        } else if (midTimestamp > timestamp) {
            long nextHigh = mid - 1;
            if (nextHigh <= low) {
                return low;
            }
            return closestBinarySearch(low, nextHigh, timestamp);
        } else {
            return mid;
        }
    }

    @Override
    public long getBackFileSize() {
        try {
            arrayReadLock().lock();
            return _getBackFileSize();
        } finally {
            arrayReadLock().unlock();
        }
    }

    @Override
    public void limitBackFileSize(long sizeLimit) {
        if (sizeLimit < INDEX_PAGE_SIZE + DATA_PAGE_SIZE) {
            return; // ignore, one index page + one data page are minimum for big array to work correctly
        }

        long backFileSize = getBackFileSize();
        if (backFileSize <= sizeLimit) {
            return; // nothing to do
        }

        long toTruncateSize = backFileSize - sizeLimit;
        if (toTruncateSize < DATA_PAGE_SIZE) {
            return; // can't do anything
        }

        try {
            arrayWriteLock().lock();

            // double check
            backFileSize = _getBackFileSize();
            if (backFileSize <= sizeLimit) return; // nothing to do

            toTruncateSize = backFileSize - sizeLimit;
            if (toTruncateSize < DATA_PAGE_SIZE) {
                return; // can't do anything
            }

            long tailIndex = arrayTailIndex.get();
            long headIndex = arrayHeadIndex.get();
            long totalLength = 0L;
            while (true) {
                if (tailIndex == headIndex) break;
                totalLength += getDataItemLength(tailIndex);
                if (totalLength > toTruncateSize) break;

                tailIndex++;

                if (mod(tailIndex, INDEX_ITEMS_PER_PAGE_BITS) == 0) { // take index page into account
                    totalLength += INDEX_PAGE_SIZE;
                }
            }
            removeBeforeIndex(tailIndex);
        } finally {
            arrayWriteLock().unlock();
        }

    }

    @Override
    public int getItemLength(long index) {
        try {
            arrayReadLock().lock();
            validateIndex(index);
            return getDataItemLength(index);
        } finally {
            arrayReadLock().unlock();
        }
    }

    private int getDataItemLength(long index) {
        ByteBuffer indexItemBuffer = getIndexItemBuffer(index);
        // position to the data item length
        int position = indexItemBuffer.position();
        indexItemBuffer.position(position + INDEX_ITEM_DATA_ITEM_LENGTH_OFFSET);
        int length = indexItemBuffer.getInt();
        return length;
    }

    // inner getBackFileSize
    private long _getBackFileSize() {
        return indexPageFactory.getBackPageFileSize() + dataPageFactory.getBackPageFileSize();
    }

    /**
     * Fast version of {@code val % 2 ^ bits}.
     */
    @VisibleForTesting
    static long mod(long val, int bits) {
        return val - ((val >> bits) << bits);
    }

    /**
     * Fast version of {@code val * 2 ^ bits}.
     */
    @VisibleForTesting
    static long mul(long val, int bits) {
        return val << bits;
    }

    /**
     * Fast version of {@code val / 2 ^ bits}.
     */
    @VisibleForTesting
    static long div(long val, int bits) {
        return val >> bits;
    }
}
