package io.spbx.storage.bigqueue;

import com.google.common.util.concurrent.ListenableFuture;
import io.spbx.util.base.annotate.CanIgnoreReturnValue;
import io.spbx.util.base.annotate.CheckReturnValue;
import io.spbx.util.collect.container.LongSize;
import io.spbx.util.io.UncheckedClosable;
import io.spbx.util.io.UncheckedFlushable;

import java.io.UncheckedIOException;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * BigQueue ADT
 */
public interface BigQueue extends LongSize, UncheckedFlushable, UncheckedClosable {
    /**
     * Returns the total number of items available in the queue.
     */
    @Override
    long size();

    /**
     * Adds a {@code data} item at the back of the queue.
     *
     * @param data data to be enqueued
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void enqueue(byte[] data) throws UncheckedIOException;

    /**
     * Adds {@code value} at the back of the queue.
     *
     * @param value integer value to be enqueued
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void enqueueInt(int value) throws UncheckedIOException;

    /**
     * Adds {@code value} at the back of the queue.
     *
     * @param value long value to be enqueued
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void enqueueLong(long value) throws UncheckedIOException;

    /**
     * Adds {@code value} at the back of the queue.
     *
     * @param value byte value to be enqueued
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void enqueueByte(byte value) throws UncheckedIOException;

    /**
     * Retrieves and removes the front of a queue as a {@code byte[]} array.
     *
     * @return data at the front of a queue
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    @CheckReturnValue
    byte[] dequeue() throws UncheckedIOException;

    /**
     * Retrieves and removes the front of a queue as a {@code int} value.
     *
     * @return data at the front of a queue cast to an {@code int}
     * @throws UncheckedIOException if there was any I/O error during the operation
     * @throws IllegalStateException if the front queue data can't be cast to an {@code int}
     */
    @CheckReturnValue
    int dequeueInt() throws UncheckedIOException, IllegalStateException;

    /**
     * Retrieves and removes the front of a queue as a {@code long} value.
     *
     * @return data at the front of a queue cast to an {@code long}
     * @throws UncheckedIOException if there was any I/O error during the operation
     * @throws IllegalStateException if the front queue data can't be cast to an {@code long}
     */
    @CheckReturnValue
    long dequeueLong() throws UncheckedIOException, IllegalStateException;

    /**
     * Retrieves and removes the front of a queue as a {@code byte} value.
     *
     * @return data at the front of a queue cast to an {@code byte}
     * @throws UncheckedIOException if there was any I/O error during the operation
     * @throws IllegalStateException if the front queue data can't be cast to an {@code byte}
     */
    @CheckReturnValue
    byte dequeueByte() throws UncheckedIOException, IllegalStateException;

    /**
     * Retrieves a Future which will complete if new Items where enqued.
     * <p>
     * Use this method to retrieve a future where to register as Listener instead of repeatedly polling the queues state.
     * On complete this future contains the result of the dequeue operation. Hence the item was automatically removed from the queue.
     *
     * @return a ListenableFuture which completes with the first entry if items are ready to be dequeued.
     */
    @CheckReturnValue
    ListenableFuture<byte[]> dequeueAsync();

    /**
     * Returns the item at the front of a queue.
     *
     * @return data at the front of a queue
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    @CheckReturnValue
    byte[] peek() throws UncheckedIOException;

    /**
     * Returns the item at the front of a queue asynchronously.
     * On complete the value set in this future is the result of the peek operation.
     * Hence, the item remains at the front of the list.
     *
     * @return a future returning the first item if available
     */
    @CheckReturnValue
    ListenableFuture<byte[]> peekAsync();

    /**
     * Iterates over all items in a queue.
     *
     * @param predicate the predicate
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    @CanIgnoreReturnValue
    <T extends Predicate<byte[]>> T iterate(T predicate) throws UncheckedIOException;

    /**
     * Iterates over all items in a queue.
     *
     * @param consumer the consumer
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    @CanIgnoreReturnValue
    <T extends Consumer<byte[]>> T forEach(T consumer) throws UncheckedIOException;

    /**
     * Removes all items from the queue.
     * This method empties the queue and deletes all back page files.
     *
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void removeAll() throws UncheckedIOException;

    /**
     * Deletes all used data files to free disk space.
     * <p>
     * BigQueue will persist enqueued data in disk files, these data files will remain even after
     * the data in them has been dequeued later, so your application is responsible to periodically call
     * this method to delete all used data files and free disk space.
     *
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void gc() throws UncheckedIOException;

    /**
     * Forces to persist all newly appended data.
     * <p>
     * Normally, you don't need to flush explicitly because:
     * <ol>
     *     <li>{@link BigQueue} automatically flushes a cached page when it is replaced out.</li>
     *     <li>{@link BigQueue} uses memory mapped file technology internally,
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
