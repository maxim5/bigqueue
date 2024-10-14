package io.spbx.storage.bigqueue.perf;

import io.spbx.storage.bigqueue.BigQueue;
import io.spbx.storage.bigqueue.BigQueueImpl;
import io.spbx.storage.bigqueue.RandomData;
import io.spbx.storage.bigqueue.perf.TestingBigqueuePerf.PerfWorker;
import io.spbx.storage.bigqueue.perf.TestingBigqueuePerf.Result;
import io.spbx.storage.bigqueue.perf.TestingBigqueuePerf.Status;
import io.spbx.util.logging.Logger;
import io.spbx.util.testing.TestSize;
import io.spbx.util.testing.ext.TempDirectoryExtension;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

@Tag("slow") @Tag("integration") @Tag("perf")
public class BigQueuePerfTest {
    @RegisterExtension private static final TempDirectoryExtension TEMP_DIRECTORY = TempDirectoryExtension.withCleanup();
    private static final Logger log = Logger.forEnclosingClass();
    private static final RandomData random = RandomData.seededForEachCallSite();
    private static final String QUEUE_NAME = "queue_name";

    // Configurable parameters
    private static final int loop = 5;
    private static final int totalItemNum = 100_000;
    private static final int producerNum = 2;
    private static final int consumerNum = 2;
    private static final int dataLength = 1024;
    private static final TestType testType = TestType.BIG_QUEUE_TEST;

    private enum TestType {
        IN_MEMORY_QUEUE_TEST,
        BIG_QUEUE_TEST,
    }

    @Test
    public void perf_test() throws Exception {
        TestSize.assumeTestSize(TestSize.STRESS);
        log.info().log("Performance test start...");

        try (BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME)) {
            for (int i = 0; i < loop; i++) {
                log.info().log("[doRunProduceThenConsume] round %d of %d", i + 1, loop);
                doRunProduceThenConsume(bigQueue);
            }

            for (int i = 0; i < loop; i++) {
                log.info().log("[doRunMixed] round %d of %d", i + 1, loop);
                doRunMixed(bigQueue);
            }
        }

        log.info().log("Performance test finished successfully");
    }

    public void doRunProduceThenConsume(BigQueue bigQueue) throws Exception {
        BlockingQueue<byte[]> memoryQueue = new LinkedBlockingQueue<>();
        Shared shared = new Shared(bigQueue, memoryQueue, new AtomicInteger(0), new AtomicInteger(0));
        CountDownLatch platch = new CountDownLatch(producerNum);
        CountDownLatch clatch = new CountDownLatch(consumerNum);
        BlockingQueue<Result> producerResults = new LinkedBlockingQueue<>();
        BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<>();

        long totalProducingTime = 0;
        long totalConsumingTime = 0;

        long start = System.currentTimeMillis();
        for (int i = 0; i < producerNum; i++) {
            Producer producer = new Producer(shared, platch, producerResults);
            producer.startInNewThread();
        }
        for (int i = 0; i < producerNum; i++) {
            Result result = producerResults.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
            totalProducingTime += result.duration;
        }
        long end = System.currentTimeMillis();

        if (testType == TestType.BIG_QUEUE_TEST) {
            assertThat(bigQueue.isEmpty()).isFalse();
        }

        System.out.println("-----------------------------------------------");
        System.out.println("Test type = " + testType);
        System.out.println("-----------------------------------------------");

        System.out.println("Producing test result:");
        System.out.println("Total test time = " + (end - start) + " ms.");
        System.out.println("Total item count = " + totalItemNum);
        System.out.println("Producer thread number = " + producerNum);
        System.out.println("Item message length = " + dataLength + " bytes");
        System.out.println("Total producing time = " + totalProducingTime + " ms.");
        System.out.println("Average producing time = " + totalProducingTime / producerNum + " ms.");
        System.out.println("-----------------------------------------------");

        start = System.currentTimeMillis();
        for (int i = 0; i < consumerNum; i++) {
            Consumer consumer = new Consumer(shared, clatch, consumerResults);
            consumer.startInNewThread();
        }
        for (int i = 0; i < consumerNum; i++) {
            Result result = consumerResults.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
            totalConsumingTime += result.duration;
        }
        end = System.currentTimeMillis();

        assertThat(memoryQueue.isEmpty()).isTrue();
        assertThat(bigQueue.isEmpty()).isTrue();

        System.out.println("Consuming test result:");
        System.out.println("Total test time = " + (end - start) + " ms.");
        System.out.println("Total item count = " + totalItemNum);
        System.out.println("Consumer thread number = " + consumerNum);
        System.out.println("Item message length = " + dataLength + " bytes");
        System.out.println("Total consuming time = " + totalConsumingTime + " ms.");
        System.out.println("Average consuming time = " + totalConsumingTime / consumerNum + " ms.");
        System.out.println("-----------------------------------------------");
    }

    public void doRunMixed(BigQueue bigQueue) throws Exception {
        BlockingQueue<byte[]> memoryQueue = new LinkedBlockingQueue<>();
        Shared shared = new Shared(bigQueue, memoryQueue, new AtomicInteger(0), new AtomicInteger(0));
        CountDownLatch allLatch = new CountDownLatch(producerNum + consumerNum);
        BlockingQueue<Result> producerResults = new LinkedBlockingQueue<>();
        BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<>();

        long totalProducingTime = 0;
        long totalConsumingTime = 0;

        long start = System.currentTimeMillis();
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
            totalProducingTime += result.duration;
        }
        for (int i = 0; i < consumerNum; i++) {
            Result result = consumerResults.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
            totalConsumingTime += result.duration;
        }
        long end = System.currentTimeMillis();

        assertThat(memoryQueue.isEmpty()).isTrue();
        assertThat(bigQueue.isEmpty()).isTrue();

        System.out.println("-----------------------------------------------");
        System.out.println("Test type = " + testType);
        System.out.println("-----------------------------------------------");

        System.out.println("Total item count = " + totalItemNum);
        System.out.println("Producer thread number = " + producerNum);
        System.out.println("Consumer thread number = " + consumerNum);
        System.out.println("Item message length = " + dataLength + " bytes");
        System.out.println("Total test time = " + (end - start) + " ms.");
        System.out.println("Test type = " + testType);
        System.out.println("Total producing time = " + totalProducingTime + " ms.");
        System.out.println("Average producing time = " + totalProducingTime / producerNum + " ms.");
        System.out.println("Total consuming time = " + totalConsumingTime + " ms.");
        System.out.println("Average consuming time = " + totalConsumingTime / consumerNum + " ms.");
        System.out.println("-----------------------------------------------");
    }

    record Shared(BigQueue bigQueue, BlockingQueue<byte[]> memoryQueue,
                  AtomicInteger producerItemsCounter, AtomicInteger consumerItemsCounter) {}

    private record Producer(Shared shared, CountDownLatch latch, Queue<Result> results) implements PerfWorker {
        @Override
        public void call() {
            byte[] data = random.randomBytes(dataLength);
            while (true) {
                int count = shared.producerItemsCounter.incrementAndGet();
                if (count > totalItemNum) {
                    break;
                }
                if (testType == TestType.IN_MEMORY_QUEUE_TEST) {
                    shared.memoryQueue.add(data);
                } else {
                    shared.bigQueue.enqueue(data);
                }
            }
        }
    }

    private record Consumer(Shared shared, CountDownLatch latch, Queue<Result> results) implements PerfWorker {
        @Override
        public void call() throws Exception {
            while (true) {
                int index = shared.consumerItemsCounter.getAndIncrement();
                if (index >= totalItemNum) {
                    break;
                }
                byte[] item;
                if (testType == TestType.IN_MEMORY_QUEUE_TEST) {
                    item = shared.memoryQueue.take();
                } else {
                    item = shared.bigQueue.dequeue();
                    while (item == null) {
                        item = shared.bigQueue.dequeue();
                    }
                }
                assertThat(item).isNotNull();
            }
        }
    }
}
