package io.spbx.storage.bigqueue;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.spbx.storage.bigqueue.page.MappedPage;
import io.spbx.storage.bigqueue.page.MappedPageFactory;
import io.spbx.storage.bigqueue.page.MappedPageFactoryImpl;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A big, fast and persistent queue implementation.
 * <p/>
 * Main features:
 * 1. FAST : close to the speed of direct memory access, both enqueue and dequeue are close to O(1) memory access.
 * 2. MEMORY-EFFICIENT : automatic paging & swapping algorithm, only most-recently accessed data is kept in memory.
 * 3. THREAD-SAFE : multiple threads can concurrently enqueue and dequeue without data corruption.
 * 4. PERSISTENT - all data in queue is persisted on disk, and is crash resistant.
 * 5. BIG(HUGE) - the total size of the queued data is only limited by the available disk space.
 */
public class BigQueueImpl implements BigQueue {
    // 2 ^ 3 = 8
    private static final int QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS = 3;
    // size in bytes of queue front index page
    private static final int QUEUE_FRONT_INDEX_PAGE_SIZE = 1 << QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS;
    // only use the first page
    private static final long QUEUE_FRONT_PAGE_INDEX = 0;
    // folder name for queue front index page
    private static final String QUEUE_FRONT_INDEX_PAGE_FOLDER = "front_index";

    private final BigArrayImpl innerArray;

    // front index of the big queue,
    private final AtomicLong queueFrontIndex = new AtomicLong();

    // factory for queue front index page management(acquire, release, cache)
    private final MappedPageFactory queueFrontIndexPageFactory;

    // locks for queue front write management
    private final Lock queueFrontWriteLock = new ReentrantLock();

    // lock for dequeueFuture access
    private final Object futureLock = new Object();
    private SettableFuture<byte[]> dequeueFuture;
    private SettableFuture<byte[]> peekFuture;

    /**
     * A big, fast and persistent queue implementation,
     * use default back data page size, see {@link BigArrayImpl#DEFAULT_DATA_PAGE_SIZE}
     *
     * @param queueDir  the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     */
    public BigQueueImpl(Path queueDir, String queueName) {
        this(queueDir, queueName, BigArrayImpl.DEFAULT_DATA_PAGE_SIZE);
    }

    /**
     * A big, fast and persistent queue implementation.
     *
     * @param queueDir  the directory to store queue data
     * @param queueName the name of the queue, will be appended as last part of the queue directory
     * @param pageSize  the back data file size per page in bytes, see minimum allowed {@link BigArrayImpl#MINIMUM_DATA_PAGE_SIZE}
     */
    public BigQueueImpl(Path queueDir, String queueName, int pageSize) {
        innerArray = new BigArrayImpl(queueDir, queueName, pageSize);

        // the ttl does not matter here since queue front index page is always cached
        queueFrontIndexPageFactory = new MappedPageFactoryImpl(
            QUEUE_FRONT_INDEX_PAGE_SIZE,
            innerArray.arrayDirectory().resolve(QUEUE_FRONT_INDEX_PAGE_FOLDER),
            innerArray.executor()
        );
        MappedPage queueFrontIndexPage = queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);

        ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocalBuffer();
        long front = queueFrontIndexBuffer.getLong();
        queueFrontIndex.set(front);
    }

    @Override
    public boolean isEmpty() {
        return queueFrontIndex.get() == innerArray.getHeadIndex();
    }

    @Override
    public void enqueue(byte[] data) {
        innerArray.append(data);
        completeFutures();
    }

    @Override
    public byte[] dequeue() {
        try {
            queueFrontWriteLock.lock();
            if (isEmpty()) {
                return null;
            }
            long queueFrontIndexValue = queueFrontIndex.get();
            byte[] data = innerArray.get(queueFrontIndexValue);
            long nextQueueFrontIndex = queueFrontIndexValue;
            if (nextQueueFrontIndex == Long.MAX_VALUE) {
                nextQueueFrontIndex = 0L; // wrap
            } else {
                nextQueueFrontIndex++;
            }
            queueFrontIndex.set(nextQueueFrontIndex);
            // persist the queue front
            MappedPage queueFrontIndexPage = queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocalBuffer();
            queueFrontIndexBuffer.putLong(nextQueueFrontIndex);
            queueFrontIndexPage.setDirty(true);
            return data;
        } finally {
            queueFrontWriteLock.unlock();
        }

    }

    @Override
    public ListenableFuture<byte[]> dequeueAsync() {
        initializeDequeueFutureIfNecessary();
        return dequeueFuture;
    }

    @Override
    public void removeAll() {
        try {
            queueFrontWriteLock.lock();
            innerArray.removeAll();
            queueFrontIndex.set(0L);
            MappedPage queueFrontIndexPage = queueFrontIndexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
            ByteBuffer queueFrontIndexBuffer = queueFrontIndexPage.getLocalBuffer();
            queueFrontIndexBuffer.putLong(0L);
            queueFrontIndexPage.setDirty(true);
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public byte[] peek() {
        if (isEmpty()) {
            return null;
        }
        return innerArray.get(queueFrontIndex.get());
    }

    @Override
    public ListenableFuture<byte[]> peekAsync() {
        initializePeekFutureIfNecessary();
        return peekFuture;
    }

    @Override
    public void applyForEach(ItemConsumer consumer) {
        try {
            queueFrontWriteLock.lock();
            if (isEmpty()) {
                return;
            }
            long index = queueFrontIndex.get();
            for (long i = index; i < innerArray.size(); i++) {
                consumer.accept(innerArray.get(i));
            }
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public void close() {
        if (queueFrontIndexPageFactory != null) {
            queueFrontIndexPageFactory.releaseCachedPages();
        }

        synchronized (futureLock) {
            // Cancel the future but don't interrupt running tasks
            // because they might perform further work not referring to the queue
            if (peekFuture != null) {
                peekFuture.cancel(false);
            }
            if (dequeueFuture != null) {
                dequeueFuture.cancel(false);
            }
        }

        innerArray.close();
    }

    @Override
    public void gc() {
        long beforeIndex = queueFrontIndex.get();
        if (beforeIndex == 0L) { // wrap
            beforeIndex = Long.MAX_VALUE;
        } else {
            beforeIndex--;
        }
        try {
            innerArray.removeBeforeIndex(beforeIndex);
        } catch (IndexOutOfBoundsException e) {
            // ignore
        }
    }

    @Override
    public void flush() {
        try {
            queueFrontWriteLock.lock();
            queueFrontIndexPageFactory.flush();
            innerArray.flush();
        } finally {
            queueFrontWriteLock.unlock();
        }
    }

    @Override
    public long size() {
        long qFront = queueFrontIndex.get();
        long qRear = innerArray.getHeadIndex();
        if (qFront <= qRear) {
            return (qRear - qFront);
        } else {
            return Long.MAX_VALUE - qFront + 1 + qRear;
        }
    }

    /**
     * Completes the dequeue future
     */
    private void completeFutures() {
        synchronized (futureLock) {
            if (peekFuture != null && !peekFuture.isDone()) {
                try {
                    peekFuture.set(peek());
                } catch (Throwable e) {
                    peekFuture.setException(e);
                }
            }
            if (dequeueFuture != null && !dequeueFuture.isDone()) {
                try {
                    dequeueFuture.set(dequeue());
                } catch (Throwable e) {
                    dequeueFuture.setException(e);
                }
            }
        }
    }

    /**
     * Initializes the futures if it's null at the moment
     */
    private void initializeDequeueFutureIfNecessary() {
        synchronized (futureLock) {
            if (dequeueFuture == null || dequeueFuture.isDone()) {
                dequeueFuture = SettableFuture.create();
            }
            if (!isEmpty()) {
                try {
                    dequeueFuture.set(dequeue());
                } catch (Throwable e) {
                    dequeueFuture.setException(e);
                }
            }
        }
    }

    /**
     * Initializes the futures if it's null at the moment
     */
    private void initializePeekFutureIfNecessary() {
        synchronized (futureLock) {
            if (peekFuture == null || peekFuture.isDone()) {
                peekFuture = SettableFuture.create();
            }
            if (!isEmpty()) {
                try {
                    peekFuture.set(peek());
                } catch (Throwable e) {
                    peekFuture.setException(e);
                }
            }
        }
    }
}
