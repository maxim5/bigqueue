package io.spbx.storage.bigqueue.page;

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
        byte[] data = new byte[length];
        buf.get(data);
        return data;
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
     * Sets or unsets the dirty bit meaning that the mapped page has changed.
     */
    void setDirty(boolean dirty);

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
