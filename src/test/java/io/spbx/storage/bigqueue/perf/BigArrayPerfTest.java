package io.spbx.storage.bigqueue.perf;

import io.spbx.storage.bigqueue.BigArray;
import io.spbx.storage.bigqueue.BigArrayImpl;
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

import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

@Tag("slow") @Tag("integration") @Tag("perf")
public class BigArrayPerfTest {
    @RegisterExtension private static final TempDirectoryExtension TEMP_DIRECTORY = TempDirectoryExtension.withCleanup();
    private static final Logger log = Logger.forEnclosingClass();
    private static final RandomData random = RandomData.seededForEachCallSite();
    private static final String ARRAY_NAME = "array_name";

    // Configurable parameters
    private static final int loop = 5;
    private static final int totalItemNum = 100_000;
    private static final int producerNum = 1;
    private static final int consumerNum = 1;
    private static final int dataLength = 1024;

    @Test
    public void perf_test() throws Exception {
        TestSize.assumeTestSize(TestSize.STRESS);
        log.info().log("Performance test start...");

        try (BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME)) {
            for (int i = 0; i < loop; i++) {
                log.info().log("[doRunProduceThenConsume] round %d of %d", i + 1, loop);
                doRunProduceThenConsume(bigArray);
                bigArray.removeAll();
            }

            for (int i = 0; i < loop; i++) {
                log.info().log("[doRunMixed] round %d of %d", i + 1, loop);
                doRunMixed(bigArray);
                bigArray.removeAll();
            }
        }

        log.info().log("Performance test finished successfully");
    }

    private static void doRunProduceThenConsume(BigArray bigArray) throws Exception {
        Shared shared = new Shared(bigArray, new AtomicInteger(0));
        CountDownLatch producerLatch = new CountDownLatch(producerNum);
        CountDownLatch consumerLatch = new CountDownLatch(consumerNum);
        BlockingQueue<Result> producerResults = new LinkedBlockingQueue<>();
        BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<>();

        long totalProducingTime = 0;
        long totalConsumingTime = 0;

        long start = System.currentTimeMillis();
        for (int i = 0; i < producerNum; i++) {
            Producer producer = new Producer(shared, producerLatch, producerResults);
            producer.startInNewThread();
        }
        for (int i = 0; i < producerNum; i++) {
            Result result = producerResults.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
            totalProducingTime += result.duration;
        }
        long end = System.currentTimeMillis();

        assertThat(bigArray.size()).isEqualTo(totalItemNum);

        System.out.println("-----------------------------------------------");
        System.out.println("Producing test result:");
        System.out.println("Total test time = " + (end - start) + " ms.");
        System.out.println("Total items produced = " + totalItemNum);
        System.out.println("Producer thread number = " + producerNum);
        System.out.println("Item message length = " + dataLength + " bytes");
        System.out.println("Total producing time = " + totalProducingTime + " ms.");
        System.out.println("Average producing time = " + totalProducingTime / producerNum + " ms.");
        System.out.println("-----------------------------------------------");

        start = System.currentTimeMillis();
        for (int i = 0; i < consumerNum; i++) {
            RandomConsumer consumer = new RandomConsumer(shared, consumerLatch, consumerResults);
            consumer.startInNewThread();
        }
        for (int i = 0; i < consumerNum; i++) {
            Result result = consumerResults.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
            totalConsumingTime += result.duration;
        }
        end = System.currentTimeMillis();

        System.out.println("-----------------------------------------------");
        System.out.println("Random consuming test result:");
        System.out.println("Total test time = " + (end - start) + " ms.");
        System.out.println("Total items consumed = " + totalItemNum);
        System.out.println("Consumer thread number = " + consumerNum);
        System.out.println("Item message length = " + dataLength + " bytes");
        System.out.println("Total consuming time = " + totalConsumingTime + " ms.");
        System.out.println("Average consuming time = " + totalConsumingTime / consumerNum + " ms.");
        System.out.println("-----------------------------------------------");

        start = System.currentTimeMillis();
        for (int i = 0; i < consumerNum; i++) {
            SequentialConsumer consumer = new SequentialConsumer(shared, consumerLatch, consumerResults);
            consumer.startInNewThread();
        }
        for (int i = 0; i < consumerNum; i++) {
            Result result = consumerResults.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
            totalConsumingTime += result.duration;
        }
        end = System.currentTimeMillis();

        System.out.println("-----------------------------------------------");
        System.out.println("Sequential consuming test result:");
        System.out.println("Total test time = " + (end - start) + " ms.");
        System.out.println("Total items consumed = " + totalItemNum);
        System.out.println("Consumer thread number = " + consumerNum);
        System.out.println("Item message length = " + dataLength + " bytes");
        System.out.println("Total consuming time = " + totalConsumingTime + " ms.");
        System.out.println("Average consuming time = " + totalConsumingTime / consumerNum + " ms.");
        System.out.println("-----------------------------------------------");

        assertThat(bigArray.size()).isEqualTo(totalItemNum);
    }

    private static void doRunMixed(BigArray bigArray) throws Exception {
        Shared shared = new Shared(bigArray, new AtomicInteger(0));

        CountDownLatch allLatch = new CountDownLatch(producerNum + consumerNum);
        BlockingQueue<Result> producerResults = new LinkedBlockingQueue<>();
        BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<>();

        long totalProducingTime = 0;
        long totalConsumingTime = 0;

        long start = System.currentTimeMillis();
        // run testing
        for (int i = 0; i < producerNum; i++) {
            Producer producer = new Producer(shared, allLatch, producerResults);
            producer.startInNewThread();
        }

        for (int i = 0; i < consumerNum; i++) {
            SequentialConsumer consumer = new SequentialConsumer(shared, allLatch, consumerResults);
            consumer.startInNewThread();
        }

        // verify
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

        assertThat(bigArray.size()).isEqualTo(totalItemNum);

        System.out.println("-----------------------------------------------");

        System.out.println("Total item count = " + totalItemNum);
        System.out.println("Producer thread number = " + producerNum);
        System.out.println("Consumer thread number = " + consumerNum);
        System.out.println("Item message length = " + dataLength + " bytes");
        System.out.println("Total test time = " + (end - start) + " ms.");
        System.out.println("Total producing time = " + totalProducingTime + " ms.");
        System.out.println("Average producing time = " + totalProducingTime / producerNum + " ms.");
        System.out.println("Total consuming time = " + totalConsumingTime + " ms.");
        System.out.println("Average consuming time = " + totalConsumingTime / consumerNum + " ms.");
        System.out.println("-----------------------------------------------");
    }

    record Shared(BigArray bigArray, AtomicInteger producedItemsCounter) {}

    private record Producer(Shared shared, CountDownLatch latch, Queue<Result> results) implements PerfWorker {
        @Override
        public void call() {
            byte[] data = random.randomBytes(dataLength);
            while (true) {
                int count = shared.producedItemsCounter.incrementAndGet();
                if (count > totalItemNum) {
                    break;
                }
                shared.bigArray.append(data);
            }
        }
    }

    // random consumer can only work after producer
    private record RandomConsumer(Shared shared, CountDownLatch latch, Queue<Result> results) implements PerfWorker {
        @Override
        public void call() {
            List<Long> indexes = random.randomPermutation(totalItemNum);
            for (long index : indexes) {
                byte[] ignore = shared.bigArray.get(index);
            }
        }
    }

    // sequential consumer can only work concurrently with producer
    private record SequentialConsumer(Shared shared, CountDownLatch latch, Queue<Result> results) implements PerfWorker {
        @Override
        public void call() {
            for (long index = 0; index < totalItemNum; index++) {
                while (index >= shared.bigArray.getHeadIndex()) {
                    TestingBasics.waitFor(20);  // no item to consume yet, just wait a moment
                }
                byte[] ignore = shared.bigArray.get(index);
            }
        }
    }
}
