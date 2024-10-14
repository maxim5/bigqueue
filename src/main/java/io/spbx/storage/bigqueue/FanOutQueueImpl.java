package io.spbx.storage.bigqueue;

import io.spbx.storage.bigqueue.page.MappedPage;
import io.spbx.storage.bigqueue.page.MappedPageFactory;
import io.spbx.storage.bigqueue.page.MappedPageFactoryImpl;
import io.spbx.util.logging.Logger;

import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import static io.spbx.util.base.BasicExceptions.newIllegalArgumentException;

/**
 * A big, fast and persistent queue implementation supporting fan out semantics.
 *  
 * Main features:
 * 1. FAST : close to the speed of direct memory access, both enqueue and dequeue are close to O(1) memory access.
 * 2. MEMORY-EFFICIENT : automatic paging & swapping algorithm, only most-recently accessed data is kept in memory.
 * 3. THREAD-SAFE : multiple threads can concurrently enqueue and dequeue without data corruption.
 * 4. PERSISTENT - all data in queue is persisted on disk, and is crash resistant.
 * 5. BIG(HUGE) - the total size of the queued data is only limited by the available disk space.
 * 6. FANOUT - support fan out semantics, multiple consumers can independently consume a single queue without intervention, 
 *                     everyone has its own queue front index.
 * 7. CLIENT MANAGED INDEX - support access by index and the queue index is managed at client side.
 */
public class FanOutQueueImpl implements FanOutQueue {
	private static final Logger log = Logger.forEnclosingClass();

	// 2 ^ 3 = 8
	private static final int QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS = 3;
	// size in bytes of queue front index page
	private static final int QUEUE_FRONT_INDEX_PAGE_SIZE = 1 << QUEUE_FRONT_INDEX_ITEM_LENGTH_BITS;
	// only use the first page
	private static final long QUEUE_FRONT_PAGE_INDEX = 0;

	// folder name prefix for queue front index page
	private static final String QUEUE_FRONT_INDEX_PAGE_FOLDER_PREFIX = "front_index_";

	private final BigArrayImpl innerArray;
	private final ConcurrentMap<String, QueueFront> queueFrontMap = new ConcurrentHashMap<>();

	/**
	 * A big, fast and persistent queue implementation with fanout support.
	 * 
	 * @param queueDir  the directory to store queue data
	 * @param queueName the name of the queue, will be appended as last part of the queue directory
	 * @param pageSize the back data file size per page in bytes, see minimum allowed {@link BigArrayImpl#MINIMUM_DATA_PAGE_SIZE}
	 * @throws UncheckedIOException exception throws if there is any IO error during queue initialization
	 */
	public FanOutQueueImpl(Path queueDir, String queueName, int pageSize) {
		innerArray = new BigArrayImpl(queueDir, queueName, pageSize);
	}

	/**
     * A big, fast and persistent queue implementation with fanout support,
     * use default back data page size, see {@link BigArrayImpl#DEFAULT_DATA_PAGE_SIZE}
	 * 
	 * @param queueDir the directory to store queue data
	 * @param queueName the name of the queue, will be appended as last part of the queue directory
	 * @throws UncheckedIOException exception throws if there is any IO error during queue initialization
	 */
	public FanOutQueueImpl(Path queueDir, String queueName) {
		this(queueDir, queueName, BigArrayImpl.DEFAULT_DATA_PAGE_SIZE);
	}

	@Override
	public long size() {
		return innerArray.size();
	}

	@Override
	public long size(String fanoutId) {
		try {
			innerArray.arrayReadLock().lock();
			QueueFront qf = getQueueFront(fanoutId);
			long qFront = qf.index.get();
			long qRear = innerArray.getHeadIndex();
			if (qFront <= qRear) {
				return qRear - qFront;
			} else {
				return Long.MAX_VALUE - qFront + 1 + qRear;
			}
		} finally {
			innerArray.arrayReadLock().unlock();
		}
	}

	@Override
	public long enqueue(byte[] data) {
		return innerArray.append(data);
	}

	@Override
	public byte[] dequeue(String fanoutId) {
		try {
			innerArray.arrayReadLock().lock();
			QueueFront qf = getQueueFront(fanoutId);
			try {
				qf.writeLock.lock();
				if (qf.index.get() == innerArray.arrayHeadIndex.get()) {
					return null; // empty
				}
				byte[] data = innerArray.get(qf.index.get());
				qf.incrementIndex();
				return data;
			} catch (IndexOutOfBoundsException e) {
				log.debug().withCause(e).log("Failed to dequeue %s from the array. Resetting QueueFront", fanoutId);
				qf.resetIndex(); // maybe the back array has been truncated to limit size
				byte[] data = innerArray.get(qf.index.get());
				qf.incrementIndex();
				return data;
			} finally {
				qf.writeLock.unlock();
			}
		} finally {
			innerArray.arrayReadLock().unlock();
		}
	}

	@Override
	public byte[] peek(String fanoutId) {
		try {
			innerArray.arrayReadLock().lock();
			QueueFront qf = getQueueFront(fanoutId);
			if (qf.index.get() == innerArray.getHeadIndex()) {
				return null; // empty
			}
			return innerArray.get(qf.index.get());
		} finally {
			innerArray.arrayReadLock().unlock();
		}
	}

	@Override
	public int peekLength(String fanoutId) {
		try {
			innerArray.arrayReadLock().lock();
			QueueFront qf = getQueueFront(fanoutId);
			if (qf.index.get() == innerArray.getHeadIndex()) {
				return -1; // empty
			}
			return innerArray.getItemLength(qf.index.get());
		} finally {
			innerArray.arrayReadLock().unlock();
		}
	}
	
	@Override
	public long peekTimestamp(String fanoutId) {
		try {
			innerArray.arrayReadLock().lock();
			QueueFront qf = getQueueFront(fanoutId);
			if (qf.index.get() == innerArray.getHeadIndex()) {
				return -1; // empty
			}
			return innerArray.getTimestamp(qf.index.get());
		} finally {
			innerArray.arrayReadLock().unlock();
		}
	}

	@Override
	public byte[] get(long index) {
		return innerArray.get(index);
	}

	@Override
	public int getLength(long index) {
		return innerArray.getItemLength(index);
	}

	@Override
	public long getTimestamp(long index) {
		return innerArray.getTimestamp(index);
	}

	@Override
	public void removeBefore(long timestamp) {
		try {
			innerArray.arrayWriteLock().lock();
			innerArray.removeBefore(timestamp);
			for (QueueFront qf : queueFrontMap.values()) {
				try {
					qf.writeLock.lock();
					qf.validateAndAdjustIndex();	
				} finally {
					qf.writeLock.unlock();
				}
			}
		} finally {
			innerArray.arrayWriteLock().unlock();
		}
	}

	@Override
	public void limitBackFileSize(long sizeLimit) {
		try {
			innerArray.arrayWriteLock().lock();
			innerArray.limitBackFileSize(sizeLimit);
			for (QueueFront qf : queueFrontMap.values()) {
				try {
					qf.writeLock.lock();
					qf.validateAndAdjustIndex();	
				} finally {
					qf.writeLock.unlock();
				}
			}
		} finally {
			innerArray.arrayWriteLock().unlock();
		}
	}

	@Override
	public long getBackFileSize() {
		return innerArray.getBackFileSize();
	}

	@Override
	public long findClosestIndex(long timestamp) {
		try {
			innerArray.arrayReadLock().lock();
			if (timestamp == LATEST) {
				return innerArray.getHeadIndex();
			}
			if (timestamp == EARLIEST) {
				return innerArray.getTailIndex();
			}
			return innerArray.findClosestIndex(timestamp);
		} finally {
			innerArray.arrayReadLock().unlock();
		}
	}

	@Override
	public void resetQueueFrontIndex(String fanoutId, long index) {
		try {
			innerArray.arrayReadLock().lock();
			QueueFront qf = getQueueFront(fanoutId);
			try {
				qf.writeLock.lock();
				if (index != innerArray.getHeadIndex()) { // ok to set index to array head index
					innerArray.validateIndex(index);
				}
				qf.index.set(index);
				qf.persistIndex();
			} finally {
				qf.writeLock.unlock();
			}
		} finally {
			innerArray.arrayReadLock().unlock();
		}
	}

	@Override
	public void flush() {
		try {
			innerArray.arrayReadLock().lock();
			
			for (QueueFront qf : queueFrontMap.values()) {
				try {
					qf.writeLock.lock();
					qf.indexPageFactory.flush();		
				} finally {
					qf.writeLock.unlock();
				}
			}
			innerArray.flush();
		} finally {
			innerArray.arrayReadLock().unlock();
		}
	}

	@Override
	public void close() {
		try {
			innerArray.arrayWriteLock().lock();
			for (QueueFront qf : queueFrontMap.values()) {
				qf.indexPageFactory.releaseCachedPages();
			}
			innerArray.close();
		} finally {
			innerArray.arrayWriteLock().unlock();
		}
	}
	
	@Override
	public void removeAll() {
		try {
			innerArray.arrayWriteLock().lock();
			for (QueueFront qf : queueFrontMap.values()) {
				try {
					qf.writeLock.lock();
					qf.index.set(0L);
					qf.persistIndex();
				} finally {
					qf.writeLock.unlock();
				}
			}
			innerArray.removeAll();
		} finally {
			innerArray.arrayWriteLock().unlock();
		}
	}

	private QueueFront getQueueFront(String fanoutId) {
		QueueFront qf = queueFrontMap.get(fanoutId);
		if (qf == null) { // not in cache, need to create one
			qf = new QueueFront(fanoutId);
			QueueFront found = queueFrontMap.putIfAbsent(fanoutId, qf);
			if (found != null) {
				qf.indexPageFactory.releaseCachedPages();
				qf = found;
			}
		}
		return qf;
	}
	
	// Queue front wrapper
	private class QueueFront {
		// fanout id
		private final String fanoutId;
		
		// front index of the fanout queue
		private final AtomicLong index = new AtomicLong();
		
		// factory for queue front index page management(acquire, release, cache)
		private final MappedPageFactory indexPageFactory;
		
		// lock for queue front write management
		private final Lock writeLock = new ReentrantLock();
		
		QueueFront(String fanoutId) {
			this.fanoutId = validateFanoutId(fanoutId);
			// the ttl does not matter here since queue front index page is always cached
			indexPageFactory = new MappedPageFactoryImpl(
				QUEUE_FRONT_INDEX_PAGE_SIZE,
				innerArray.arrayDirectory().resolve(QUEUE_FRONT_INDEX_PAGE_FOLDER_PREFIX + fanoutId),
				innerArray.executor()
			);
			
			MappedPage indexPage = indexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
			ByteBuffer indexBuffer = indexPage.getLocalBuffer();
			index.set(indexBuffer.getLong());
			validateAndAdjustIndex();
		}

		private static final Pattern ILLEGAL_CHARS_PATTERN =
			Pattern.compile("(^\\.{1,2}$)|[/\u0000\u0001-\u001F\u007F-\u009F\uD800-\uF8FF\uFFF0-\uFFFF]");

		private static String validateFanoutId(String id) {
			if (id == null || id.isEmpty()) {
				throw newIllegalArgumentException("Fanout id is empty");
			}
			if(id.length() > 255) {
				throw newIllegalArgumentException("Fanout id is too long");
			}
			if (ILLEGAL_CHARS_PATTERN.matcher(id).find()) {
				throw newIllegalArgumentException("Fanout id `%s` contains illegal chars", id);
			}
			return id;
		}
		
		void validateAndAdjustIndex() {
			if (index.get() != innerArray.arrayHeadIndex.get()) { // ok that index is equal to array head index
				try {
					innerArray.validateIndex(index.get());
				} catch (IndexOutOfBoundsException e) { // maybe the back array has been truncated to limit size
					resetIndex();
				}
		   }
		}
		
		// reset queue front index to the tail of array
		void resetIndex() {
			index.set(innerArray.arrayTailIndex.get());
			
			persistIndex();
		}
		
		void incrementIndex() {
			long nextIndex = index.get();
			if (nextIndex == Long.MAX_VALUE) {
				nextIndex = 0L; // wrap
			} else {
				nextIndex++;
			}
			index.set(nextIndex);
			
			persistIndex();
		}
		
		void persistIndex() {
			// persist index
			MappedPage indexPage = indexPageFactory.acquirePage(QUEUE_FRONT_PAGE_INDEX);
			ByteBuffer indexBuffer = indexPage.getLocalBuffer();
			indexBuffer.putLong(index.get());
			indexPage.setDirty(true);
		}
	}

	@Override
	public long getFrontIndex() {
		return innerArray.getTailIndex();
	}

	@Override
	public long getRearIndex() {
		return innerArray.getHeadIndex();
	}

	@Override
	public long getFrontIndex(String fanoutId) {
		try {
			innerArray.arrayReadLock().lock();
			QueueFront qf = getQueueFront(fanoutId);
			return qf.index.get();
		} finally {
			innerArray.arrayReadLock().unlock();
		}
	}
}
