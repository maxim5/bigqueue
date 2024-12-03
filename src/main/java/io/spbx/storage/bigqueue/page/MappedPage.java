package io.spbx.storage.bigqueue.page;

import io.spbx.util.base.ops.ByteOps;
import io.spbx.util.io.UncheckedClosable;
import io.spbx.util.io.UncheckedFlushable;

import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * Memory mapped page file ADT
 */
public interface MappedPage extends UncheckedFlushable, UncheckedClosable {
    /**
     * Returns a thread local copy of the mapped page buffer.
     *
     * @param position start position (relative to the start position of source mapped page buffer) of the result buffer
     */
    ByteBuffer getLocalBuffer(int position);

    /**
     * Returns a thread local copy of the mapped page buffer started at {@code 0} position.
     */
    default ByteBuffer getLocalBuffer() {
        return getLocalBuffer(0);
    }

    /**
     * Returns the data byte array from the mapped page buffer.
     *
     * @param position start position (relative to the start position of source mapped page buffer) of the result buffer
     * @param length   the length to fetch
     */
    default byte[] getBufferBytes(int position, int length) {
        ByteBuffer buf = getLocalBuffer(position);
        return ByteOps.getBytes(buf, length);
    }

    /**
     * Returns the back page file name of the mapped page.
     */
    Path getPageFile();

    /**
     * Returns the index of the mapped page.
     */
    long getPageIndex();

    /**
     * Sets or unsets the dirty bit, i.e. whether the mapped page has changed.
     */
    void setDirty(boolean dirty);

    /**
     * Returns the dirty bit, i.e. whether the mapped page has changed.
     */
    boolean isDirty();

    /**
     * Returns whether this mapped page is closed.
     */
    boolean isClosed();

    /**
     * Persists any changes onto the file system.
     */
    @Override
    void flush();
}
