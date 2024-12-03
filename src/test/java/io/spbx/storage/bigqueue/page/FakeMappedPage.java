package io.spbx.storage.bigqueue.page;

import io.spbx.util.logging.Logger;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

public class FakeMappedPage implements MappedPage {
    private static final Logger log = Logger.forEnclosingClass();

    private final ByteBuffer buffer;
    private final long index;
    private final AtomicBoolean dirty = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    public FakeMappedPage(@NotNull ByteBuffer buffer, long index) {
        log.debug().log("FakeMappedPage(%s, %s)", buffer, index);
        this.buffer = buffer;
        this.index = index;
    }

    @Override
    public ByteBuffer getLocalBuffer(int position) {
        return buffer.position(position);
    }

    @Override
    public Path getPageFile() {
        return Path.of("/tmp/" + this);
    }

    @Override
    public long getPageIndex() {
        return index;
    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty.set(dirty);
    }

    @Override
    public boolean isDirty() {
        return dirty.get();
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void flush() {
        // Do nothing
        dirty.compareAndSet(true, false);
    }

    @Override
    public void close() {
        closed.compareAndSet(false, true);
    }

    public @NotNull String toString() {
        return "mapped-page-%d".formatted(index);
    }
}
