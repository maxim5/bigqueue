package io.spbx.storage.bigqueue.page;

import io.spbx.util.io.UncheckedClosable;
import io.spbx.util.testing.TestingBasics;
import io.spbx.util.testing.concurrent.TraceExecutor;
import org.jetbrains.annotations.NotNull;

class Resource implements UncheckedClosable {
    private volatile boolean closed = false;

    public static Resource newResource() {
        return new Resource();
    }

    @Override
    public void close() {
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    static void waitForExpire(long tll) {
        TestingBasics.waitFor(tll + 1);
    }

    static void waitForExpire(@NotNull TraceExecutor executor, long tll) {
        TestingBasics.waitFor(tll + 1);
        executor.waitForAllDone();
    }
}
