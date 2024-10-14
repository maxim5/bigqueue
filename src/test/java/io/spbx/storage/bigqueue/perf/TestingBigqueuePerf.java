package io.spbx.storage.bigqueue.perf;

import io.spbx.util.logging.Logger;
import io.spbx.util.testing.TestingThreads.ThreadWorker;
import io.spbx.util.time.TimeIt;

import java.util.Queue;
import java.util.concurrent.CountDownLatch;

class TestingBigqueuePerf {
    private static final Logger log = Logger.forEnclosingClass();

    public enum Status {
        ERROR,
        SUCCESS,
    }

    public static class Result {
        Status status;
        long duration;
    }

    public interface PerfWorker extends ThreadWorker {
        CountDownLatch latch();
        Queue<Result> results();

        void call() throws Exception;

        @Override default void run() {
            Result result = new Result();
            try {
                latch().countDown();
                latch().await();

                TimeIt.timeIt(this::call).onDone(millis -> {
                    result.status = Status.SUCCESS;
                    result.duration = millis;
                });
            } catch (Exception e) {
                log.warn().withCause(e).log("%s failed: %s", getClass().getSimpleName(), e.getMessage());
                result.status = Status.ERROR;
            }
            results().offer(result);
        }
    }
}
