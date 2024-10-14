package io.spbx.storage.bigqueue.perf;

import io.spbx.storage.bigqueue.BigQueue;
import io.spbx.storage.bigqueue.BigQueueImpl;
import io.spbx.storage.bigqueue.RandomData;
import io.spbx.storage.bigqueue.perf.TestingBigqueuePerf.PerfWorker;
import io.spbx.storage.bigqueue.perf.TestingBigqueuePerf.Result;
import io.spbx.storage.bigqueue.perf.TestingBigqueuePerf.Status;
import io.spbx.util.logging.Logger;
import io.spbx.util.testing.TestSize;
import io.spbx.util.testing.TestingBasics;
import io.spbx.util.testing.ext.TempDirectoryExtension;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

@Tag("slow") @Tag("integration") @Tag("stress")
public class BigQueueLoadTest {
    @RegisterExtension private static final TempDirectoryExtension TEMP_DIRECTORY = TempDirectoryExtension.withCleanup();
    private static final Logger log = Logger.forEnclosingClass();
    private static final RandomData random = RandomData.seededForEachCallSite();
    private static final String QUEUE_NAME = "queue_name";

    // Configurable parameters
    private static final int loop = 5;
    private static final int totalItemNum = 100_000;
    private static final int producerNum = 4;
    private static final int consumerNum = 4;
    private static final int dataLength = 1024;

    @Test
    public void load_test() throws Exception {
        TestSize.assumeTestSize(TestSize.STRESS);
        log.info().log("Load test start...");

        try (BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME)) {
            for (int i = 0; i < loop; i++) {
                log.info().log("[doRunProduceThenConsume] round %d of %d", i + 1, loop);
                doRunProduceThenConsume(bigQueue);
                bigQueue.gc();
            }
        }

        try (BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME)) {
            for (int i = 0; i < loop; i++) {
                log.info().log("[doRunMixed] round %d of %d", i + 1, loop);
                doRunMixed(bigQueue);
                bigQueue.gc();
            }
        }

        log.info().log("Load test finished successfully");
    }

    private static void doRunProduceThenConsume(BigQueue bigQueue) throws Exception {
        Set<String> items = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Shared shared = new Shared(bigQueue, new AtomicInteger(0), new AtomicInteger(0), items);
        CountDownLatch producerLatch = new CountDownLatch(producerNum);
        CountDownLatch consumerLatch = new CountDownLatch(consumerNum);
        BlockingQueue<Result> producerResults = new LinkedBlockingQueue<>();
        BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<>();

        for (int i = 0; i < producerNum; i++) {
            Producer producer = new Producer(shared, producerLatch, producerResults);
            producer.startInNewThread();
        }
        for (int i = 0; i < producerNum; i++) {
            Result result = producerResults.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
        }

        bigQueue.flush();
        assertThat(items.size()).isEqualTo(totalItemNum);
        assertThat(bigQueue.isEmpty()).isFalse();
        assertThat(bigQueue.size()).isEqualTo(totalItemNum);

        for (int i = 0; i < consumerNum; i++) {
            Consumer consumer = new Consumer(shared, consumerLatch, consumerResults);
            consumer.startInNewThread();
        }
        for (int i = 0; i < consumerNum; i++) {
            Result result = consumerResults.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
        }

        assertThat(items.isEmpty()).isTrue();
        assertThat(bigQueue.isEmpty()).isTrue();
        assertThat(bigQueue.size()).isEqualTo(0);
    }

    private static void doRunMixed(BigQueue bigQueue) throws Exception {
        Set<String> items = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Shared shared = new Shared(bigQueue, new AtomicInteger(0), new AtomicInteger(0), items);

        CountDownLatch allLatch = new CountDownLatch(producerNum + consumerNum);
        BlockingQueue<Result> producerResults = new LinkedBlockingQueue<>();
        BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<>();

        for (int i = 0; i < producerNum; i++) {
            Producer producer = new Producer(shared, allLatch, producerResults);
            producer.startInNewThread();
        }
        for (int i = 0; i < consumerNum; i++) {
            Consumer consumer = new Consumer(shared, allLatch, consumerResults);
            consumer.startInNewThread();
        }

        for (int i = 0; i < producerNum; i++) {
            Result result = producerResults.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
        }
        for (int i = 0; i < consumerNum; i++) {
            Result result = consumerResults.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
        }

        assertThat(items.isEmpty()).isTrue();
        assertThat(bigQueue.isEmpty()).isTrue();
        assertThat(bigQueue.size()).isEqualTo(0);
    }

    record Shared(BigQueue bigQueue, AtomicInteger producerItemsCounter, AtomicInteger consumerItemsCounter, Set<String> items) {}

    private record Producer(Shared shared, CountDownLatch latch, Queue<Result> results) implements PerfWorker {
        @Override
        public void call() {
            String data = random.randomString(dataLength);
            while (true) {
                int count = shared.producerItemsCounter.incrementAndGet();
                if (count > totalItemNum) {
                    break;
                }
                String item = data + count;
                shared.items.add(item);
                shared.bigQueue.enqueue(item.getBytes());
            }
        }
    }

    private record Consumer(Shared shared, CountDownLatch latch, Queue<Result> results) implements PerfWorker {
        @Override
        public void call() {
            while (true) {
                int count = shared.consumerItemsCounter.getAndIncrement();
                if (count >= totalItemNum) {
                    break;
                }
                byte[] data = shared.bigQueue.dequeue();
                while (data == null) {
                    TestingBasics.waitFor(10);
                    data = shared.bigQueue.dequeue();
                }
                String item = new String(data);
                assertThat(shared.items.remove(item)).isTrue();
            }
        }
    }
}
