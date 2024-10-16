package io.spbx.storage.bigqueue;

import io.spbx.util.logging.Logger;
import io.spbx.util.testing.TestingBasics;
import io.spbx.util.testing.ext.TempDirectoryExtension;
import io.spbx.util.time.TimeIt;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import static com.google.common.truth.Truth.assertThat;
import static io.spbx.storage.bigqueue.BigArrayImpl.MINIMUM_DATA_PAGE_SIZE;
import static io.spbx.storage.bigqueue.TestingData.strBytesOf;
import static io.spbx.util.base.BasicArrays.fill;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("slow") @Tag("integration")
public class FanOutQueueIntegrationTest {
    @RegisterExtension private static final TempDirectoryExtension TEMP_DIRECTORY = TempDirectoryExtension.withCleanup();
    private static final Logger log = Logger.forEnclosingClass();
    private static final String QUEUE_NAME = "queue_name";
    private static final int TIMEOUT_SMALL = Timeouts.timeout(200, 500);

    private final RandomData random = RandomData.withSharedSeed();

    @Test
    public void simple_ops_small() {
        String fid = "simple_ops_small";
        for (int i = 1; i <= 2; i++) {
            try (FanOutQueue queue = new FanOutQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME)) {
                assertThat(queue).isNotNull();

                for (int j = 1; j <= 3; j++) {
                    assertThat(queue.size(fid)).isEqualTo(0L);
                    assertThat(queue.isEmpty(fid)).isTrue();

                    assertThat(queue.dequeue(fid)).isNull();
                    assertThat(queue.peek(fid)).isNull();

                    queue.enqueue("hello".getBytes());
                    assertThat(queue.size(fid)).isEqualTo(1L);
                    assertThat(queue.isEmpty(fid)).isFalse();
                    assertThat(new String(queue.peek(fid))).isEqualTo("hello");
                    assertThat(new String(queue.dequeue(fid))).isEqualTo("hello");
                    assertThat(queue.dequeue(fid)).isNull();

                    queue.enqueue("world".getBytes());
                    queue.flush();
                    assertThat(queue.size(fid)).isEqualTo(1L);
                    assertThat(queue.isEmpty(fid)).isFalse();
                    assertThat(new String(queue.dequeue(fid))).isEqualTo("world");
                    assertThat(queue.dequeue(fid)).isNull();
                }
            }
        }
    }

    @Test
    public void simple_ops_indexes_big() {
        FanOutQueue queue = new FanOutQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME);
        assertThat(queue).isNotNull();
        assertThat(queue.isEmpty()).isTrue();

        int loop = 100_000;
        for (int i = 0; i < loop; i++) {
            queue.enqueue(strBytesOf(i));
            assertThat(i + 1L).isEqualTo(queue.size());
            assertThat(queue.isEmpty()).isFalse();
            assertThat(new String(queue.get(i))).isEqualTo("" + i);
        }
    }

    @Test
    public void simple_ops_big() {
        String fid1 = "simple_ops1";
        String fid2 = "simple_ops2";
        int loop = 100_000;

        try (FanOutQueue queue = new FanOutQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME)) {
            assertThat(queue).isNotNull();

            long ts = -1;
            for (int i = 0; i < loop; i++) {
                queue.enqueue(strBytesOf(i));
                assertThat(i + 1L).isEqualTo(queue.size(fid1));
                assertThat(queue.isEmpty(fid1)).isFalse();
                byte[] data = queue.peek(fid1);
                assertThat(new String(data)).isEqualTo("0");
                int length = queue.peekLength(fid1);
                assertThat(length).isEqualTo(1);
                if (ts == -1) {
                    ts = queue.peekTimestamp(fid1);
                } else {
                    assertThat(queue.peekTimestamp(fid1)).isEqualTo(ts);
                }
            }

            assertThat(loop).isEqualTo(queue.size(fid1));
            assertThat(queue.isEmpty(fid1)).isFalse();
            assertThat(new String(queue.peek(fid1))).isEqualTo("0");
        }

        // create a new instance over the exiting queue
        try (FanOutQueue queue = new FanOutQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME)) {
            assertThat(loop).isEqualTo(queue.size(fid1));
            assertThat(queue.isEmpty(fid1)).isFalse();

            for (int i = 0; i < loop; i++) {
                byte[] data = queue.dequeue(fid1);
                assertThat(new String(data)).isEqualTo("" + i);
                assertThat(loop - i - 1).isEqualTo(queue.size(fid1));
            }
            assertThat(queue.isEmpty(fid1)).isTrue();

            // fan out test

            assertThat(loop).isEqualTo(queue.size(fid2));
            assertThat(queue.isEmpty(fid2)).isFalse();
            assertThat(new String(queue.peek(fid2))).isEqualTo("0");

            for (int i = 0; i < loop; i++) {
                byte[] data = queue.dequeue(fid2);
                assertThat(new String(data)).isEqualTo("" + i);
                assertThat(loop - i - 1).isEqualTo(queue.size(fid2));
            }
            assertThat(queue.isEmpty(fid2)).isTrue();
        }
    }

    @Test
    public void simple_access_timed() {
        String fid1 = "loopTimingTest1";
        String fid2 = "loopTimingTest2";
        int loop = 1000_000;
        byte[][] data = fill(new byte[loop][], TestingData::strBytesOf);

        FanOutQueue queue = new FanOutQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME);
        assertThat(queue).isNotNull();

        TimeIt.timeIt(() -> {
            for (int i = 0; i < loop; i++) {
                queue.enqueue(data[i]);
            }
        }).onDone(millis -> log.debug().log("Time used to enqueue %d items : %d millis", loop, millis));

        TimeIt.timeIt(() -> {
            for (int i = 0; i < loop; i++) {
                assertThat(queue.dequeue(fid1)).isEqualTo(data[i]);
            }
        }).onDone(millis -> log.debug().log("Fanout test 1: Time used to dequeue %d items : %d millis", loop, millis));

        TimeIt.timeIt(() -> {
            for (int i = 0; i < loop; i++) {
                assertThat(queue.dequeue(fid2)).isEqualTo(data[i]);
            }
        }).onDone(millis -> log.debug().log("Fanout test 2: Time used to dequeue %d items : %d millis", loop, millis));
    }

    @Test
    @SuppressWarnings("resource")
    public void invalid_data_page_size() {
        new FanOutQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE);  // ok
        assertThrows(IllegalArgumentException.class, () -> {
            new FanOutQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE - 1);
        });
    }

    @Test
    public void resetQueueFrontIndex_simple() {
        FanOutQueue queue = new FanOutQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME);
        assertThat(queue).isNotNull();
        String fid = "resetQueueFrontIndex";

        int loop = 100_000;
        for (int i = 0; i < loop; i++) {
            queue.enqueue(strBytesOf(i));
        }
        assertThat(new String(queue.peek(fid))).isEqualTo("0");

        queue.resetQueueFrontIndex(fid, 1L);
        assertThat(new String(queue.peek(fid))).isEqualTo("1");

        queue.resetQueueFrontIndex(fid, 1234L);
        assertThat(new String(queue.peek(fid))).isEqualTo("1234");

        queue.resetQueueFrontIndex(fid, loop - 1);
        assertThat(new String(queue.peek(fid))).isEqualTo((loop - 1) + "");

        queue.resetQueueFrontIndex(fid, loop);
        assertThat(queue.peek(fid)).isNull();

        assertThrows(IndexOutOfBoundsException.class, () -> queue.resetQueueFrontIndex(fid, loop + 1));
    }

    @Test
    public void removeBefore_simple() {
        FanOutQueue queue = new FanOutQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE);
        assertThat(queue).isNotNull();

        String randomString1 = random.randomString(32);
        for (int i = 0; i < 1024 * 1024; i++) {
            queue.enqueue(randomString1.getBytes());
        }

        String fid = "removeBeforeTest";
        assertThat(queue.size(fid)).isEqualTo(1024 * 1024);

        long timestamp = System.currentTimeMillis();
        String randomString2 = random.randomString(32);
        for (int i = 0; i < 1024 * 1024; i++) {
            queue.enqueue(randomString2.getBytes());
        }

        queue.removeBefore(timestamp);
        timestamp = System.currentTimeMillis();
        String randomString3 = random.randomString(32);
        for (int i = 0; i < 1024 * 1024; i++) {
            queue.enqueue(randomString3.getBytes());
        }

        queue.removeBefore(timestamp);
        assertThat(queue.size(fid)).isEqualTo(9 * 128 * 1024);
        assertThat(new String(queue.peek(fid))).isEqualTo(randomString2);
    }

    @Test
    public void findClosestIndex_simple() {
        FanOutQueue queue = new FanOutQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE);
        assertThat(queue).isNotNull();

        assertThat(queue.findClosestIndex(System.currentTimeMillis())).isEqualTo(-1);

        int loop = 100_000;
        long begin = System.currentTimeMillis();
        TestingBasics.waitFor(TIMEOUT_SMALL);
        for (int i = 0; i < loop; i++) {
            queue.enqueue(strBytesOf(i));
        }
        long midTs1 = System.currentTimeMillis();
        for (int i = 0; i < loop; i++) {
            queue.enqueue(strBytesOf(i));
        }
        long midTs2 = System.currentTimeMillis();
        for (int i = 0; i < loop; i++) {
            queue.enqueue(strBytesOf(i));
        }

        TestingBasics.waitFor(TIMEOUT_SMALL);
        long end = System.currentTimeMillis();

        assertThat(queue.findClosestIndex(begin)).isEqualTo(0L);
        assertThat(queue.findClosestIndex(end)).isEqualTo(3 * loop - 1);

        assertThat(queue.findClosestIndex(FanOutQueue.EARLIEST)).isEqualTo(0L);
        assertThat(queue.findClosestIndex(FanOutQueue.LATEST)).isEqualTo(3 * loop);

        long midIndex1 = queue.findClosestIndex(midTs1);
        long midIndex2 = queue.findClosestIndex(midTs2);
        assertThat(0L < midIndex1).isTrue();
        assertThat(midIndex1 < midIndex2).isTrue();
        assertThat(3 * loop - 1 > midIndex2).isTrue();

        long closestTime = queue.getTimestamp(midIndex1);
        long closestTimeBefore = queue.getTimestamp(midIndex1 - 1);
        long closestTimeAfter = queue.getTimestamp(midIndex1 + 1);
        assertThat(closestTimeBefore <= closestTime).isTrue();
        assertThat(closestTimeAfter >= closestTime).isTrue();
    }

    @Test
    public void findClosestIndex_all() {
        FanOutQueue queue = new FanOutQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE);
        assertThat(queue).isNotNull();
        assertThat(queue.findClosestIndex(System.currentTimeMillis())).isEqualTo(-1);

        int loop = 100;
        long[] tsArray = new long[loop];
        for (int i = 0; i < loop; i++) {
            TestingBasics.waitFor(1);
            queue.enqueue(strBytesOf(i));
            tsArray[i] = System.currentTimeMillis();
        }

        for (int i = 0; i < loop; i++) {
            int index = (int) queue.findClosestIndex(tsArray[i]);
            assertThat(index).isAnyOf(i, i + 1);
        }
    }

    @Test
    public void limitBackFileSize_simple() {
        FanOutQueue queue = new FanOutQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE);
        assertThat(queue).isNotNull();

        int total = 1024 * 1024;  // 1 Mb
        byte[] data1 = random.randomBytes(32);
        byte[] data2 = random.randomBytes(32);
        byte[] data3 = random.randomBytes(32);

        for (int i = 0; i < total; i++) {       // 5 data pages + 5 index page
            queue.enqueue(data1);
        }
        for (int i = 0; i < total; i++) {       // 5 data pages + 5 index page
            queue.enqueue(data2);
        }
        for (int i = 0; i < total; i++) {       // 5 data pages + 5 index page
            queue.enqueue(data3);
        }

        assertThat(queue.size("test")).isEqualTo(3 * total);
        assertThat(queue.dequeue("test")).isEqualTo(data1);
        assertThat(queue.get(0)).isEqualTo(data1);
        assertThat(queue.getBackFileSize()).isEqualTo(6 * 32 * total);

        queue.limitBackFileSize(total * 4 * 32);
        TestingBasics.waitFor(TIMEOUT_SMALL);
        assertThat(queue.size("test")).isEqualTo(2 * total);
        assertThat(queue.getBackFileSize()).isEqualTo(4 * 32 * total);
        assertThat(queue.dequeue("test")).isEqualTo(data2);

        queue.limitBackFileSize(total * 2 * 32);
        TestingBasics.waitFor(TIMEOUT_SMALL);
        assertThat(queue.size("test")).isEqualTo(total);
        assertThat(queue.getBackFileSize()).isEqualTo(2 * 32 * total);
        assertThat(queue.dequeue("test")).isEqualTo(data3);

        queue.limitBackFileSize(total * 32); // will be ignored
        TestingBasics.waitFor(TIMEOUT_SMALL);
        assertThat(queue.size("test")).isEqualTo(total - 1);
        assertThat(queue.getBackFileSize()).isEqualTo(2 * 32 * total);
        assertThat(queue.dequeue("test")).isEqualTo(data3);
    }
}
