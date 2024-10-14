package io.spbx.storage.bigqueue;

import io.spbx.util.collect.LongSize;
import io.spbx.util.io.UncheckedClosable;
import io.spbx.util.io.UncheckedFlushable;

import java.io.UncheckedIOException;

/**
 * Append Only Big Array.
 */
public interface BigArray extends LongSize, UncheckedFlushable, UncheckedClosable {
    /**
     * Returns the total number of items in the array.
     */
    @Override
    long size();

    /**
     * Appends the {@code data} into the head of the array.
     *
     * @param data binary data to append
     * @return appended index
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    long append(byte[] data) throws UncheckedIOException;

    /**
     * Returns the data at the specific {@code index}.
     *
     * @param index valid data index
     * @return binary data if the index is valid
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    byte[] get(long index) throws UncheckedIOException;

    /**
     * Returns the timestamp of data at the specific {@code index},
     * i.e. the timestamp when the data was appended.
     *
     * @param index valid data index
     * @return timestamp when the data was appended
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    long getTimestamp(long index) throws UncheckedIOException;

    /**
     * Returns the back data file size per page.
     *
     * @return size per page
     */
    int getDataPageSize();

    /**
     * The head of the array.
     * <p>
     * This is the next index to append, the index of the last appended data
     * is {@code [headIndex - 1]} if the array is not empty.
     *
     * @return an index
     */
    long getHeadIndex();

    /**
     * The tail of the array.
     * <p>
     * This is the index of the first appended data.
     *
     * @return an index
     */
    long getTailIndex();

    /**
     * Removes all data in this array.
     * This method empties the array and deletes all back page files.
     *
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void removeAll() throws UncheckedIOException;

    /**
     * Removes all data before the yhe specific {@code index}.
     * This method advances the array tail to {@code index} and deletes back page files before index.
     *
     * @param index an index
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void removeBeforeIndex(long index) throws UncheckedIOException;

    /**
     * Remove all data before the specific {@code timestamp}.
     * This method advances the array tail and deletes back page files accordingly.
     *
     * @param timestamp a timestamp
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void removeBefore(long timestamp) throws UncheckedIOException;

    /**
     * Returns the index of an item so that its append timestamp is closest
     * to the specific {@code timestamp}, or {@code -1} if not found.
     *
     * @param timestamp to look up
     * @return an index
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    long findClosestIndex(long timestamp) throws UncheckedIOException;

    /**
     * Returns the total size of back files (index and data files) of this array.
     *
     * @return total size of back files
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    long getBackFileSize() throws UncheckedIOException;

    /**
     * Limits the back file size at {@code sizeLimit}, truncates back file and
     * advances the array tail index accordingly.
     * Note that this is the best effort call, exact size limit can't be guaranteed.
     *
     * @param sizeLimit the size to limit
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void limitBackFileSize(long sizeLimit) throws UncheckedIOException;

    /**
     * Returns the data item length at specific {@code index}.
     *
     * @param index valid data index
     * @return the length of binary data if the index is valid
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    int getItemLength(long index) throws UncheckedIOException;

    /**
     * Forces to persist all newly appended data.
     * <p>
     * Normally, you don't need to flush explicitly because:
     * <ol>
     *     <li>{@link BigArray} automatically flushes a cached page when it is replaced out.</li>
     *     <li>{@link BigArray} uses memory mapped file technology internally,
     *         and the OS will flush the changes even if the process crashes.</li>
     * </ol>
     * <p>
     * Call this periodically only if you need transactional reliability,
     * and you are aware of the performance cost.
     *
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    @Override
    void flush() throws UncheckedIOException;
}
