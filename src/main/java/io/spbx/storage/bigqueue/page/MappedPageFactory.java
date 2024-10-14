package io.spbx.storage.bigqueue.page;

import io.spbx.util.io.UncheckedFlushable;

import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Set;

/**
 * Memory mapped page management ADT
 */
public interface MappedPageFactory extends UncheckedFlushable {
    /**
     * Acquires and returns a mapped page for the specific {@code index} from the factory.
     * <p>
     * Once done, a calling thread should {@link #releasePage(long)} to inform the factory
     * to recycle the page and make it available to other clients.
     *
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    MappedPage acquirePage(long index) throws UncheckedIOException;

    /**
     * Releases the previously acquired mapped page to the factory.
     * <p>
     * Once done, a calling thread should {@code #releasePage()} to inform the factory
     * to recycle the page and make it available to other clients.
     */
    void releasePage(long index);

    /**
     * Returns the current set page size. That is the size which the factory uses when creating pages.
     */
    int getPageSize();

    /**
     * Returns the current page directory.
     */
    Path getPageDir();

    /**
     * Deletes the mapped page with the specific {@code index}.
     * This call removes the page from the cache if it was cached and deletes the back file.
     *
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void deletePage(long index) throws UncheckedIOException;

    /**
     * Delete mapped pages with the specific {@code indexes}.
     * This call removes the pages from the cache if they were cached and deletes the back files.
     *
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void deletePages(Set<Long> indexes) throws UncheckedIOException;

    /**
     * Deletes all mapped pages currently available in this factory.
     * This call removes all cached pages from the cache and deletes the back files.
     *
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void deleteAllPages() throws UncheckedIOException;

    /**
     * Removes all cached pages from the cache and closes resources associated with the cached pages.
     *
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void releaseCachedPages() throws UncheckedIOException;

    /**
     * Deletes all pages with last modified timestamp before the specific {@code timestamp}.
     *
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void deletePagesBefore(long timestamp) throws UncheckedIOException;

    /**
     * Delete all pages before the specific {@code pageIndex}.
     *
     * @throws UncheckedIOException if there was any I/O error during the operation
     */
    void deletePagesBeforePageIndex(long pageIndex) throws UncheckedIOException;

    /**
     * Returns the indexes of pages with last modified timestamp before the specific {@code timestamp}.
     */
    Set<Long> getPageIndexSetBefore(long timestamp);

    /**
     * Returns the last modified timestamp of the specified page {@code index}.
     */
    long getPageFileLastModifiedTime(long index);

    /**
     * Returns the index of a page file with last modified timestamp closest to specific {@code timestamp}.
     */
    long getFirstPageIndexBefore(long timestamp);

    /**
     * Returns the list of indexes of the currently existing back files.
     */
    Set<Long> getExistingBackFileIndexSet();

    /**
     * Returns the current cache size in bytes.
     */
    int getCacheSize();

    /**
     * Returns a set of the file names of the back page.
     */
    Set<String> getBackPageFileSet();

    /**
     * Returns the total size of all page files.
     */
    long getBackPageFileSize();

    /**
     * Persists any changes onto the file system.
     */
    @Override
    void flush();
}
