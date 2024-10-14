package io.spbx.storage.bigqueue;

import io.spbx.util.collect.LongSize;
import io.spbx.util.io.UncheckedClosable;
import io.spbx.util.io.UncheckedFlushable;

import java.io.UncheckedIOException;

/**
 * FanOut queue ADT.
 */
public interface FanOutQueue extends LongSize, UncheckedFlushable, UncheckedClosable {
    /**
     * Constant represents the earliest timestamp
     */
    long EARLIEST = -1;
    /**
     * Constant represents the latest timestamp
     */
    long LATEST = -2;

    /**
     * Returns the total number of items remaining in the queue.
     */
    @Override
    long size();

    /**
     * Returns whether the queue is empty.
     */
    @Override
    default boolean isEmpty() {
        return LongSize.super.isEmpty();
    }

    /**
     * Total number of items remaining in the fan out queue.
     *
     * @param fanoutId the fanout identifier
     * @return total number
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    long size(String fanoutId) throws UncheckedIOException;

    /**
     * Returns whether a fan out queue is empty.
     *
     * @param fanoutId the fanout identifier
     * @return true if empty, false otherwise
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    default boolean isEmpty(String fanoutId) throws UncheckedIOException {
        return size(fanoutId) == 0;
    }

    /**
     * Adds a {@code data} item at the back of the queue.
     *
     * @param data data to be enqueued
     * @return index where the item was appended
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    long enqueue(byte[] data) throws UncheckedIOException;

    /**
     * Returns and removes the front of a fan out queue.
     *
     * @param fanoutId the fanout identifier
     * @return data at the front of a queue
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    byte[] dequeue(String fanoutId) throws UncheckedIOException;

    /**
     * Peeks the item at the front of a fanout queue, without removing it from the queue
     *
     * @param fanoutId the fanout identifier
     * @return data at the front of a queue
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    byte[] peek(String fanoutId) throws UncheckedIOException;

    /**
     * Peeks the length of the item at the front of a fan out queue.
     *
     * @param fanoutId the fanout identifier
     * @return data at the front of a queue
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    int peekLength(String fanoutId) throws UncheckedIOException;

    /**
     * Peeks the timestamp of the item at the front of a fan out queue.
     *
     * @param fanoutId the fanout identifier
     * @return data at the front of a queue
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    long peekTimestamp(String fanoutId) throws UncheckedIOException;

    /**
     * Returns data item at the specific {@code index} of the queue.
     *
     * @param index data item index
     * @return data at index
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    byte[] get(long index) throws UncheckedIOException;

    /**
     * Returns length of data item at specific {@code index} of the queue.
     *
     * @param index data item index
     * @return length of data item
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    int getLength(long index) throws UncheckedIOException;

    /**
     * Returns the timestamp of data item at specific {@code index} of the queue, i.e. the timestamp
     * when corresponding item was appended into the queue.
     *
     * @param index data item index
     * @return timestamp of data item
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    long getTimestamp(long index) throws UncheckedIOException;

    /**
     * Removes all data before specific {@code timestamp},
     * truncates back files and advances the queue front if necessary.
     *
     * @param timestamp a timestamp
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void removeBefore(long timestamp) throws UncheckedIOException;

    /**
     * Limits the back file size of this queue, truncates back files and advances the queue front if necessary.
     * <p>
     * Note that this is the best effort call, exact size limit can't be guaranteed.
     *
     * @param sizeLimit size limit
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void limitBackFileSize(long sizeLimit) throws UncheckedIOException;

    /**
     * Returns the current total size of the back files of this queue.
     *
     * @return total back file size
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    long getBackFileSize() throws UncheckedIOException;

    /**
     * Returns the index of an item so that its enqueue timestamp is closest
     * to the specific {@code timestamp}, or {@code -1} if not found.
     * <p>
     * For the latest index, use {@link #LATEST} as timestamp.
     * For the earliest index, use {@link #EARLIEST} as timestamp.
     *
     * @param timestamp when the corresponding item was appended
     * @return an index
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    long findClosestIndex(long timestamp) throws UncheckedIOException;

    /**
     * Resets the front index of a fanout queue.
     *
     * @param fanoutId fanout identifier
     * @param index    target index
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void resetQueueFrontIndex(String fanoutId, long index) throws UncheckedIOException;

    /**
     * Removes all items from the queue.
     * This method empties the queue and deletes all back page files.
     *
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void removeAll() throws UncheckedIOException;

    /**
     * Returns the queue front index, this is the earliest appended index.
     *
     * @return an index
     */
    long getFrontIndex();

    /**
     * Returns front index of specific fanout queue.
     *
     * @param fanoutId fanout identifier
     * @return an index
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    long getFrontIndex(String fanoutId) throws UncheckedIOException;

    /**
     * Returns the queue rear index, this is the next to be appended index.
     *
     * @return an index
     */
    long getRearIndex();

    /**
     * Forces to persist all newly appended data.
     * <p>
     * Normally, you don't need to flush explicitly because:
     * <ol>
     *     <li>{@link FanOutQueue} automatically flushes a cached page when it is replaced out.</li>
     *     <li>{@link FanOutQueue} uses memory mapped file technology internally,
     *         and the OS will flush the changes even if the process crashes.</li>
     * </ol>
     * <p>
     * Call this periodically only if you need transactional reliability,
     * and you are aware of the performance cost.
     *
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void flush();
}
