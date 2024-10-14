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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.truth.Truth.assertThat;

@Tag("slow") @Tag("integration") @Tag("stress")
public class BigArrayLoadTest {
    @RegisterExtension private static final TempDirectoryExtension TEMP_DIRECTORY = TempDirectoryExtension.withCleanup();
    private static final Logger log = Logger.forEnclosingClass();
    private static final RandomData random = RandomData.seededForEachCallSite();
    private static final String ARRAY_NAME = "array_name";

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

        try (BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME)) {
            for (int i = 0; i < loop; i++) {
                log.info().log("[doRunProduceThenConsume] round %d of %d", i + 1, loop);
                doRunProduceThenConsume(bigArray);
                bigArray.removeAll();
            }
        }

        try (BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME)) {
            for (int i = 0; i < loop; i++) {
                log.info().log("[doRunMixed] round %d of %d", i + 1, loop);
                doRunMixed(bigArray);
                bigArray.removeAll();
            }
        }

        log.info().log("Load test finished successfully");
    }

    private static void doRunProduceThenConsume(BigArray bigArray) throws Exception {
        ConcurrentMap<String, AtomicInteger> items = new ConcurrentHashMap<>();
        Shared shared = new Shared(bigArray, new AtomicInteger(0), items);
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

        bigArray.flush();
        assertThat(bigArray.size()).isEqualTo(totalItemNum);
        assertThat(items.size()).isEqualTo(totalItemNum);

        for (int i = 0; i < consumerNum; i++) {
            RandomConsumer consumer = new RandomConsumer(shared, consumerLatch, consumerResults);
            consumer.startInNewThread();
        }
        for (int i = 0; i < consumerNum; i++) {
            Result result = consumerResults.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
        }

        assertThat(bigArray.size()).isEqualTo(totalItemNum);
        assertThat(items.size()).isEqualTo(totalItemNum);
        for (AtomicInteger counter : items.values()) {
            assertThat(counter.get()).isEqualTo(consumerNum);
        }
    }

    private static void doRunMixed(BigArray bigArray) throws Exception {
        ConcurrentMap<String, AtomicInteger> items = new ConcurrentHashMap<>();
        Shared shared = new Shared(bigArray, new AtomicInteger(0), items);

        CountDownLatch allLatch = new CountDownLatch(producerNum + consumerNum);
        BlockingQueue<Result> producerResults = new LinkedBlockingQueue<>();
        BlockingQueue<Result> consumerResults = new LinkedBlockingQueue<>();

        for (int i = 0; i < producerNum; i++) {
            Producer producer = new Producer(shared, allLatch, producerResults);
            producer.startInNewThread();
        }
        for (int i = 0; i < consumerNum; i++) {
            SequentialConsumer consumer = new SequentialConsumer(shared, allLatch, consumerResults);
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

        assertThat(bigArray.size()).isEqualTo(totalItemNum);
        assertThat(items.size()).isEqualTo(totalItemNum);
        for (AtomicInteger counter : items.values()) {
            assertThat(counter.get()).isEqualTo(consumerNum);
        }
    }

    record Shared(BigArray bigArray, AtomicInteger producedItemsCounter, ConcurrentMap<String, AtomicInteger> items) {}

	private record Producer(Shared shared, CountDownLatch latch, Queue<Result> results) implements PerfWorker {
        @Override
        public void call() {
            String data = random.randomString(dataLength);
            while (true) {
                int count = shared.producedItemsCounter.incrementAndGet();
                if (count > totalItemNum) {
                    break;
                }
                String item = data + '-' + count;
                shared.items.put(item, new AtomicInteger(0));
                shared.bigArray.append(item.getBytes());
            }
        }
	}

	// random consumer can only work after producer
	private record RandomConsumer(Shared shared, CountDownLatch latch, Queue<Result> results) implements PerfWorker {
        @Override
        public void call() {
            List<Long> indexes = random.randomPermutation(totalItemNum);
            for (long index : indexes) {
                byte[] data = shared.bigArray.get(index);
                assertThat(data).isNotNull();
                String item = new String(data);
                AtomicInteger counter = shared.items.get(item);
                assertThat(counter).isNotNull();
                counter.incrementAndGet();
            }
        }
	}

	// sequential consumer can only work concurrently with producer
	private record SequentialConsumer(Shared shared, CountDownLatch latch, Queue<Result> results) implements PerfWorker {
        @Override
        public void call() {
            for (long index = 0; index < totalItemNum; index++) {
                while (index >= shared.bigArray.getHeadIndex()) {
                    TestingBasics.waitFor(20); // no item to consume yet, just wait a moment
                }
                byte[] data = shared.bigArray.get(index);
                assertThat(data).isNotNull();
                String item = new String(data);
                AtomicInteger counter = shared.items.get(item);
                assertThat(counter).isNotNull();
                counter.incrementAndGet();
            }
        }
	}
}
