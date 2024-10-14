package io.spbx.storage.bigqueue;

import com.google.common.util.concurrent.ListenableFuture;
import io.spbx.util.logging.Logger;
import io.spbx.util.testing.TestingBasics;
import io.spbx.util.testing.ext.TempDirectoryExtension;
import io.spbx.util.time.TimeIt;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static io.spbx.storage.bigqueue.BigArrayImpl.MINIMUM_DATA_PAGE_SIZE;
import static io.spbx.storage.bigqueue.TestingData.strBytesOf;
import static io.spbx.util.base.BasicArrays.fill;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

@Tag("slow") @Tag("integration")
public class BigQueueIntegrationTest {
    @RegisterExtension private static final TempDirectoryExtension TEMP_DIRECTORY = TempDirectoryExtension.withCleanup();
    private static final Logger log = Logger.forEnclosingClass();
    private static final String QUEUE_NAME = "queue_name";

    @Test
    public void simple_ops_small() {
        for (int i = 1; i <= 2; i++) {
            try (BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME)) {
                assertThat(bigQueue).isNotNull();

                for (int j = 1; j <= 3; j++) {
                    assertThat(bigQueue.size()).isEqualTo(0L);
                    assertThat(bigQueue.isEmpty()).isTrue();
                    assertThat(bigQueue.dequeue()).isNull();
                    assertThat(bigQueue.peek()).isNull();

                    bigQueue.enqueue("hello".getBytes());
                    assertThat(bigQueue.size()).isEqualTo(1L);
                    assertThat(bigQueue.isEmpty()).isFalse();
                    assertThat(new String(bigQueue.peek())).isEqualTo("hello");
                    assertThat(new String(bigQueue.dequeue())).isEqualTo("hello");
                    assertThat(bigQueue.dequeue()).isNull();

                    bigQueue.enqueue("world".getBytes());
                    bigQueue.flush();
                    assertThat(bigQueue.size()).isEqualTo(1L);
                    assertThat(bigQueue.isEmpty()).isFalse();
                    assertThat(new String(bigQueue.dequeue())).isEqualTo("world");
                    assertThat(bigQueue.dequeue()).isNull();
                }
            }
        }
    }

    @Test
    public void simple_ops_big() {
        int loop = 500_000;
        byte[][] data = fill(new byte[loop][], TestingData::strBytesOf);

        try (BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME)) {
            for (int i = 0; i < loop; i++) {
                bigQueue.enqueue(data[i]);
                assertThat(bigQueue.size()).isEqualTo(i + 1);
                assertThat(bigQueue.isEmpty()).isFalse();
                assertThat(bigQueue.peek()).isEqualTo(data[0]);
            }

            assertThat(bigQueue.size()).isEqualTo(loop);
            assertThat(bigQueue.isEmpty()).isFalse();
            assertThat(bigQueue.peek()).isEqualTo(data[0]);
        }

        try (BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME)) {
            assertThat(bigQueue.size()).isEqualTo(loop);
            assertThat(bigQueue.isEmpty()).isFalse();

            for (int i = 0; i < loop; i++) {
                assertThat(bigQueue.dequeue()).isEqualTo(data[i]);
                assertThat(loop - i - 1).isEqualTo(bigQueue.size());
            }
            assertThat(bigQueue.isEmpty()).isTrue();
        }
    }

    @Test
    public void simple_access_timed() {
        BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME);
        assertThat(bigQueue).isNotNull();
        int loop = 1000_000;
        byte[][] data = fill(new byte[loop][], TestingData::strBytesOf);

        TimeIt.timeIt(() -> {
            for (int i = 0; i < loop; i++) {
                bigQueue.enqueue(data[i]);
            }
        }).onDone(millis -> log.debug().log("Time used to enqueue %d items : %d millis", loop, millis));

        TimeIt.timeIt(() -> {
            for (int i = 0; i < loop; i++) {
                assertThat(bigQueue.dequeue()).isEqualTo(data[i]);
            }
        }).onDone(millis -> log.debug().log("Time used to dequeue %d items : %d millis", loop, millis));
    }

    @Test
    public void invalid_data_page_size() {
        assertThrows(IllegalArgumentException.class, () -> {
            new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE - 1);
        });
    }

    @Test
    public void applyForEach_read_only() {
        BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE);
        assertThat(bigQueue).isNotNull();

        bigQueue.enqueue("1".getBytes());
        bigQueue.enqueue("2".getBytes());
        bigQueue.enqueue("3".getBytes());

        DefaultItemConsumer consumer = new DefaultItemConsumer();
        bigQueue.applyForEach(consumer);
        assertThat(bigQueue.size()).isEqualTo(3);
        assertThat(consumer.count()).isEqualTo(3);

        assertThat(bigQueue.dequeue()).isEqualTo("1".getBytes());
        assertThat(bigQueue.dequeue()).isEqualTo("2".getBytes());
        assertThat(bigQueue.dequeue()).isEqualTo("3".getBytes());
        assertThat(bigQueue.size()).isEqualTo(0);
    }

    @Test
    public void applyForEach_concurrent_write() throws Exception {
        BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE);
        assertThat(bigQueue).isNotNull();
        long loop = 100_000;

        Thread publisher = new Thread(() -> {
            long item = 0;
            for (int i = 0; i < loop; i++) {
                bigQueue.enqueue(strBytesOf(++item));
                Thread.yield();
            }
        });

        Thread subscriber = new Thread(() -> {
            long item = 0;
            for (int i = 0; i < loop; i++) {
                if (!bigQueue.isEmpty()) {
                    assertThat(bigQueue.dequeue()).isEqualTo(strBytesOf(++item));
                }
                Thread.yield();
            }
        });

        subscriber.start();
        publisher.start();

        for (int i = 0; i < loop; i += (int) (loop / 100)) {
            DefaultItemConsumer consumer = new DefaultItemConsumer();
            bigQueue.applyForEach(consumer);
            TestingBasics.waitFor(2);
        }

        publisher.join();
        subscriber.join();
    }

    @Test
    public void dequeueAsync_future_completed_at_queue_listeners_called() throws Exception {
        BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE);
        assertThat(bigQueue).isNotNull();

        Executor executor1 = mock(Executor.class);
        Executor executor2 = mock(Executor.class);

        ListenableFuture<byte[]> future = bigQueue.dequeueAsync();
        future.addListener(mock(Runnable.class), executor1);
        future.addListener(mock(Runnable.class), executor2);
        verify(executor1, never()).execute(any(Runnable.class));

        bigQueue.enqueue("test".getBytes());
        bigQueue.enqueue("test2".getBytes());

        assertThat(future.isDone()).isTrue();
        assertThat(new String(future.get())).isEqualTo("test");
        verify(executor1, times(1)).execute(any(Runnable.class));
        verify(executor2, times(1)).execute(any(Runnable.class));

        ListenableFuture<byte[]> future2 = bigQueue.dequeueAsync();
        future2.addListener(mock(Runnable.class), executor1);
        assertThat(future2.isDone()).isTrue();

        try {
            byte[] entry = future2.get(5, TimeUnit.SECONDS);
            assertThat(new String(entry)).isEqualTo("test2");
        } catch (Exception e) {
            fail("Future isn't already completed though there are further entries.");
        }
    }

    @Test
    public void dequeueAsync_future_completed_listener_registered_later() {
        BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE);
        assertThat(bigQueue).isNotNull();

        Executor executor = mock(Executor.class);
        bigQueue.enqueue("test".getBytes());

        ListenableFuture<byte[]> future = bigQueue.dequeueAsync();
        assertThat(future).isNotNull();
        future.addListener(mock(Runnable.class), executor);
        verify(executor).execute(any(Runnable.class));
    }

    @Test
    public void dequeueAsync_future_recreated_after_dequeue() {
        BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE);
        assertThat(bigQueue).isNotNull();

        Executor executor = mock(Executor.class);
        bigQueue.enqueue("test".getBytes());
        ListenableFuture<byte[]> future = bigQueue.dequeueAsync();
        assertThat(future.isDone()).isTrue();

        future = bigQueue.dequeueAsync();
        assertThat(future.isDone()).isFalse();
        assertThat(future.isCancelled()).isFalse();

        future.addListener(mock(Runnable.class), executor);
        bigQueue.enqueue("test".getBytes());
        verify(executor).execute(any(Runnable.class));
    }

    @Test
    public void dequeueAsync_future_canceled_after_close() {
        BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE);
        assertThat(bigQueue).isNotNull();

        Executor executor = mock(Executor.class);
        ListenableFuture<byte[]> future = bigQueue.dequeueAsync();
        future.addListener(mock(Runnable.class), executor);
        bigQueue.close();
        assertThat(future.isCancelled()).isTrue();
    }

    @Test
    public void dequeueAsync_after_recreation() {
        try (BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE)) {
            bigQueue.enqueue("test".getBytes());
        }

        try (BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE)) {
            Executor executor = mock(Executor.class);
            ListenableFuture<byte[]> future = bigQueue.dequeueAsync();
            future.addListener(mock(Runnable.class), executor);
            assertThat(future.isDone()).isTrue();
            verify(executor).execute(any(Runnable.class));
        }
    }

    @Test
    public void dequeueAsync_future_invalid_after_deletion() {
        BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE);
        assertThat(bigQueue).isNotNull();
        bigQueue.enqueue("test".getBytes());

        Executor executor1 = mock(Executor.class);
        Executor executor2 = mock(Executor.class);

        ListenableFuture<byte[]> future = bigQueue.dequeueAsync();
        future.addListener(mock(Runnable.class), executor1);

        bigQueue.removeAll();
        future = bigQueue.dequeueAsync();
        future.addListener(mock(Runnable.class), executor2);
        verify(executor1).execute(any(Runnable.class));
        verify(executor2, never()).execute(any(Runnable.class));

        bigQueue.enqueue("test".getBytes());
        verify(executor2).execute(any(Runnable.class));
    }

    @Test
    public void dequeueAsync_peekAsync_concurrent() throws Exception {
        BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE);
        assertThat(bigQueue).isNotNull();

        ListenableFuture<byte[]> dequeueFuture = bigQueue.dequeueAsync();
        ListenableFuture<byte[]> peekFuture = bigQueue.peekAsync();
        bigQueue.enqueue("Test1".getBytes());

        assertThat(dequeueFuture.isDone()).isTrue();
        assertThat(peekFuture.isDone()).isTrue();
        assertThat(new String(dequeueFuture.get())).isEqualTo("Test1");
        assertThat(new String(peekFuture.get())).isEqualTo("Test1");
        assertThat(bigQueue.size()).isEqualTo(0);
    }

    @Test
    public void peekAsync_concurrent() throws Exception {
        BigQueue bigQueue = new BigQueueImpl(TEMP_DIRECTORY.currentTempDir(), QUEUE_NAME, MINIMUM_DATA_PAGE_SIZE);
        assertThat(bigQueue).isNotNull();

        ListenableFuture<byte[]> peekFuture1 = bigQueue.peekAsync();
        bigQueue.enqueue("Test1".getBytes());
        ListenableFuture<byte[]> peekFuture2 = bigQueue.peekAsync();
        ListenableFuture<byte[]> peekFuture3 = bigQueue.peekAsync();

        assertThat(peekFuture1.isDone()).isTrue();
        assertThat(peekFuture2.isDone()).isTrue();
        assertThat(peekFuture3.isDone()).isTrue();
        assertThat(bigQueue.size()).isEqualTo(1);
        assertThat(new String(peekFuture1.get())).isEqualTo("Test1");
        assertThat(new String(peekFuture2.get())).isEqualTo("Test1");
        assertThat(new String(peekFuture3.get())).isEqualTo("Test1");
    }

    // TODO fixed this case and make the case pass
    /* temporarily commented out this failing case
    @Test
    public void testFutureIfConsumerDequeuesAllWhenAsynchronousWriting() throws Exception {
        bigQueue = new BigQueueImpl(testDir, "testFutureIfConsumerDequeuesAllWhenAsynchronousWriting", BigArrayImpl.MINIMUM_DATA_PAGE_SIZE);
        final int numberOfItems = 10000;
        final IBigQueue spyBigQueue = spy(bigQueue);


        final Executor executor = Executors.newFixedThreadPool(2);
        final Semaphore testFlowControl = new Semaphore(2);
        testFlowControl.acquire(2);

        final ListenableFuture<byte[]> future1 = spyBigQueue.dequeueAsync();

        Runnable r = new Runnable() {
            int dequeueCount = 0;
            ListenableFuture<byte[]> future;

            @Override
            public void run() {
                byte[] item = null;
                try {
                    item = (future == null) ? future1.get() : future.get();
                    assertThat(new String(item)).isEqualTo(String.valueOf(dequeueCount));
                    dequeueCount++;
                    future = spyBigQueue.dequeueAsync();
                    future.addListener(this, executor);
                    if (dequeueCount == numberOfItems) {
                        testFlowControl.release();
                    }
                } catch (Exception e) {
                    fail("Unexpected exception during dequeue operation");
                }
            }
        };

        future1.addListener(r, executor);


        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < numberOfItems; i++) {
                    try {
                        spyBigQueue.enqueue(String.valueOf(i).getBytes());
                    } catch (Exception e) {
                        fail("Unexpected exception during enqueue operation");
                    }
                }
                testFlowControl.release();
            }
        }).start();

        if (testFlowControl.tryAcquire(2, 10, TimeUnit.SECONDS)) {
            verify(spyBigQueue, times(numberOfItems)).dequeue();
        } else {
            fail("Something is wrong with the testFlowControl semaphore or timing");
        }
    }
    */

    private static class DefaultItemConsumer implements BigQueue.ItemConsumer {
        private long count = 0;
        private final StringBuilder builder = new StringBuilder(256);

        public void accept(byte[] item) {
            if (count++ < 20) {
                builder.append(new String(item)).append(", ");
            }
        }

        public long count() {
            return count;
        }

        @Override public String toString() {
            return builder.toString();
        }
    }
}
