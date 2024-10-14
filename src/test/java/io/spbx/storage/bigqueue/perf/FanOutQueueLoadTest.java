package io.spbx.storage.bigqueue.perf;

import io.spbx.storage.bigqueue.FanOutQueue;
import io.spbx.storage.bigqueue.FanOutQueueImpl;
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
public class FanOutQueueLoadTest {
    @RegisterExtension private static final TempDirectoryExtension TEMP_DIRECTORY = TempDirectoryExtension.withCleanup();
    private static final Logger log = Logger.forEnclosingClass();
    private static final RandomData random = RandomData.seededForEachCallSite();
    private static final String QUEUE_NAME = "queue_name";

    // Configurable parameters
    private static final int loop = 5;
    private static final int totalItemNum = 100_000;
    private static final int producerNum = 4;
    private static final int consumerNumA = 2;
    private static final int consumerNumB = 4;
    private static final int dataLength = 1024;

    @Test
    public void load_test() throws Exception {
        TestSize.assumeTestSize(TestSize.STRESS);
        log.info().log("Load test start...");

        try (FanOutQueue queue = new FanOutQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME)) {
            for (int i = 0; i < loop; i++) {
                log.info().log("[doRunProduceThenConsume] round %d of %d", i + 1, loop);
                doRunProduceThenConsume(queue);
                queue.removeAll();
            }
        }

        try (FanOutQueue queue = new FanOutQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME)) {
            for (int i = 0; i < loop; i++) {
                log.info().log("[doRunMixed] round %d of %d", i + 1, loop);
                doRunMixed(queue);
                queue.removeAll();
            }
        }

        log.info().log("Load test finished successfully");
    }

    private static void doRunMixed(FanOutQueue queue) throws Exception {
        AtomicInteger producerCounter = new AtomicInteger(0);
        AtomicInteger consumerCounterA = new AtomicInteger(0);
        AtomicInteger consumerCounterB = new AtomicInteger(0);

        Set<String> itemsA = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Set<String> itemsB = Collections.newSetFromMap(new ConcurrentHashMap<>());

        String fanoutIdA = "groupA";
        String fanoutIdB = "groupB";

        CountDownLatch producerLatch = new CountDownLatch(producerNum);
        CountDownLatch consumerLatchA = new CountDownLatch(consumerNumA);
        CountDownLatch consumerLatchB = new CountDownLatch(consumerNumB);
        BlockingQueue<Result> producerResults = new LinkedBlockingQueue<>();
        BlockingQueue<Result> consumerResultsA = new LinkedBlockingQueue<>();
        BlockingQueue<Result> consumerResultsB = new LinkedBlockingQueue<>();

        for (int i = 0; i < producerNum; i++) {
            Producer producer = new Producer(queue, producerCounter, itemsA, itemsB, producerLatch, producerResults);
            producer.startInNewThread();
        }
        for (int i = 0; i < consumerNumA; i++) {
            Consumer consumer = new Consumer(queue, fanoutIdA, consumerCounterA, itemsA, consumerLatchA, consumerResultsA);
            consumer.startInNewThread();
        }
        for (int i = 0; i < consumerNumB; i++) {
            Consumer consumer = new Consumer(queue, fanoutIdB, consumerCounterB, itemsB, consumerLatchB, consumerResultsB);
            consumer.startInNewThread();
        }

        for (int i = 0; i < consumerNumA; i++) {
            Result result = consumerResultsA.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
        }
        for (int i = 0; i < consumerNumB; i++) {
            Result result = consumerResultsB.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
        }
        for (int i = 0; i < producerNum; i++) {
            Result result = producerResults.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
        }

        assertThat(itemsA.isEmpty()).isTrue();
        assertThat(queue.isEmpty(fanoutIdA)).isTrue();
        assertThat(0).isEqualTo(queue.size(fanoutIdA));

        assertThat(itemsB.isEmpty()).isTrue();
        assertThat(queue.isEmpty(fanoutIdB)).isTrue();
        assertThat(0).isEqualTo(queue.size(fanoutIdB));

        assertThat(queue.isEmpty()).isFalse();
        assertThat(queue.size()).isEqualTo(totalItemNum);
    }

    private static void doRunProduceThenConsume(FanOutQueue queue) throws Exception {
        String fanoutIdA = "groupA";
        String fanoutIdB = "groupB";
        AtomicInteger producerCounter = new AtomicInteger(0);
        AtomicInteger consumerCounterA = new AtomicInteger(0);
        AtomicInteger consumerCounterB = new AtomicInteger(0);
        Set<String> itemsA = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Set<String> itemsB = Collections.newSetFromMap(new ConcurrentHashMap<>());

        CountDownLatch producerLatch = new CountDownLatch(producerNum);
        CountDownLatch consumerLatchA = new CountDownLatch(consumerNumA);
        CountDownLatch consumerLatchB = new CountDownLatch(consumerNumB);
        BlockingQueue<Result> producerResults = new LinkedBlockingQueue<>();
        BlockingQueue<Result> consumerResultsA = new LinkedBlockingQueue<>();
        BlockingQueue<Result> consumerResultsB = new LinkedBlockingQueue<>();

        for (int i = 0; i < producerNum; i++) {
            Producer producer = new Producer(queue, producerCounter, itemsA, itemsB, producerLatch, producerResults);
            producer.startInNewThread();
        }
        for (int i = 0; i < producerNum; i++) {
            Result result = producerResults.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
        }

        queue.flush();
        assertThat(queue.isEmpty()).isFalse();
        assertThat(queue.size()).isEqualTo(totalItemNum);
        assertThat(itemsA.size()).isEqualTo(totalItemNum);
        assertThat(itemsB.size()).isEqualTo(totalItemNum);

        for (int i = 0; i < consumerNumA; i++) {
            Consumer consumer = new Consumer(queue, fanoutIdA, consumerCounterA, itemsA, consumerLatchA, consumerResultsA);
            consumer.startInNewThread();
        }
        for (int i = 0; i < consumerNumB; i++) {
            Consumer consumer = new Consumer(queue, fanoutIdB, consumerCounterB, itemsB, consumerLatchB, consumerResultsB);
            consumer.startInNewThread();
        }
        for (int i = 0; i < consumerNumA; i++) {
            Result result = consumerResultsA.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
        }
        for (int i = 0; i < consumerNumB; i++) {
            Result result = consumerResultsB.take();
            assertThat(result.status).isEqualTo(Status.SUCCESS);
        }

        assertThat(itemsA.isEmpty()).isTrue();
        assertThat(queue.isEmpty(fanoutIdA)).isTrue();
        assertThat(queue.size(fanoutIdA)).isEqualTo(0);

        assertThat(itemsB.isEmpty()).isTrue();
        assertThat(queue.isEmpty(fanoutIdB)).isTrue();
        assertThat(queue.size(fanoutIdB)).isEqualTo(0);

        assertThat(queue.isEmpty()).isFalse();
        assertThat(queue.size()).isEqualTo(totalItemNum);
    }

    private record Producer(FanOutQueue queue, AtomicInteger producerItemsCounter, Set<String> itemsA, Set<String> itemsB,
                            CountDownLatch latch, Queue<Result> results) implements PerfWorker {
        @Override
        public void call() {
            String data = random.randomString(dataLength);
            while (true) {
                int count = producerItemsCounter.incrementAndGet();
                if (count > totalItemNum) {
                    break;
                }
                String item = data + count;
                itemsA.add(item);
                itemsB.add(item);
                queue.enqueue(item.getBytes());
            }
        }
    }

    private record Consumer(FanOutQueue queue, String fanoutId, AtomicInteger consumingItemCount, Set<String> items,
                            CountDownLatch latch, Queue<Result> results) implements PerfWorker {
        @Override
        public void call() {
            while (true) {
                int index = consumingItemCount.getAndIncrement();
                if (index >= totalItemNum) {
                    break;
                }
                byte[] data = queue.dequeue(fanoutId);
                while (data == null) {
                    TestingBasics.waitFor(10);
                    data = queue.dequeue(fanoutId);
                }
                String item = new String(data);
                assertThat(item).isNotNull();
                assertThat(items.remove(item)).isTrue();
            }
        }
    }
}
