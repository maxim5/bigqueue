package io.spbx.storage.bigqueue.page;

import io.spbx.util.logging.Logger;

import javax.annotation.concurrent.ThreadSafe;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;

@ThreadSafe
class MappedPageImpl implements MappedPage {
    private static final Logger log = Logger.forEnclosingClass();

    private ThreadLocalByteBuffer threadLocalBuffer;
    private volatile boolean dirty = false;
    private volatile boolean closed = false;
    private final Path pageFile;
    private final long index;

    MappedPageImpl(MappedByteBuffer mbb, Path pageFile, long index) {
        this.threadLocalBuffer = new ThreadLocalByteBuffer(mbb);
        this.pageFile = pageFile;
        this.index = index;
    }

    @Override
    public ByteBuffer getLocalBuffer(int position) {
        ByteBuffer buf = threadLocalBuffer.get();
        buf.position(position);
        return buf;
    }

    @Override
    public Path getPageFile() {
        return pageFile;
    }

    @Override
    public long getPageIndex() {
        return index;
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public synchronized void flush() {
        if (closed) {
            return;
        }
        if (dirty) {
            MappedByteBuffer srcBuf = threadLocalBuffer.getSourceBuffer();
            srcBuf.force();  // flush the changes
            dirty = false;
            log.debug().log("Mapped page for %s was just flushed", pageFile);
        }
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        flush();
        unmap(threadLocalBuffer.getSourceBuffer());
        threadLocalBuffer = null;  // hint GC
        closed = true;
        log.debug().log("Mapped page for %s was just unmapped and closed", pageFile);
    }

    @Override
    public String toString() {
        return "Mapped page for %s, index = %d".formatted(pageFile, index);
    }

    private static void unmap(MappedByteBuffer buffer) {
        Cleaner.clean(buffer);
    }

    /**
     * Helper class allowing to clean direct buffers.
     */
    private static class Cleaner {
        public static final boolean CLEAN_SUPPORTED;
        private static final Method directBufferCleaner;
        private static final Method directBufferCleanerClean;

        static {
            Method directBufferCleanerX = null;
            Method directBufferCleanerCleanX = null;
            boolean v;
            try {
                directBufferCleanerX = Class.forName("java.nio.DirectByteBuffer").getMethod("cleaner");
                directBufferCleanerX.setAccessible(true);
                directBufferCleanerCleanX = Class.forName("sun.misc.Cleaner").getMethod("clean");
                directBufferCleanerCleanX.setAccessible(true);
                v = true;
            } catch (Exception e) {
                v = false;
            }
            CLEAN_SUPPORTED = v;
            directBufferCleaner = directBufferCleanerX;
            directBufferCleanerClean = directBufferCleanerCleanX;
        }

        public static void clean(ByteBuffer buffer) {
            if (buffer == null) return;
            if (CLEAN_SUPPORTED && buffer.isDirect()) {
                try {
                    Object cleaner = directBufferCleaner.invoke(buffer);
                    directBufferCleanerClean.invoke(cleaner);
                } catch (Exception e) {
                    // silently ignore exception
                }
            }
        }
    }

    private static class ThreadLocalByteBuffer extends ThreadLocal<ByteBuffer> {
        private final MappedByteBuffer buffer;

        public ThreadLocalByteBuffer(MappedByteBuffer src) {
            buffer = src;
        }

        public MappedByteBuffer getSourceBuffer() {
            return buffer;
        }

        @Override protected synchronized ByteBuffer initialValue() {
            return buffer.duplicate();
        }
    }
}
