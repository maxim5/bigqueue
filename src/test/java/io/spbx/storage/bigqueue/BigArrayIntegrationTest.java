package io.spbx.storage.bigqueue;

import io.spbx.util.logging.Logger;
import io.spbx.util.testing.TestSize;
import io.spbx.util.testing.TestingBasics;
import io.spbx.util.testing.ext.CloseAllExtension;
import io.spbx.util.testing.ext.TempDirectoryExtension;
import io.spbx.util.time.TimeIt;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;
import static io.spbx.storage.bigqueue.BigArrayImpl.*;
import static io.spbx.storage.bigqueue.TestingData.strBytesOf;
import static io.spbx.util.base.BasicArrays.fill;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag("slow") @Tag("integration")
public class BigArrayIntegrationTest {
    @RegisterExtension private static final TempDirectoryExtension TEMP_DIRECTORY = TempDirectoryExtension.withCleanup();
    @RegisterExtension private static final CloseAllExtension CLOSE_ALL = new CloseAllExtension();
    private static final Logger log = Logger.forEnclosingClass();
    private static final String ARRAY_NAME = "array_name";

    private final RandomData random = RandomData.withSharedSeed();

    @Test
    public void simple_ops_small() {
        BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME);
        assertThat(bigArray).isNotNull();

        for (int i = 1; i <= 3; i++) {
            assertThat(bigArray.getTailIndex()).isEqualTo(0L);
            assertThat(bigArray.getHeadIndex()).isEqualTo(0L);
            assertThat(bigArray.size()).isEqualTo(0L);
            assertThat(bigArray.isEmpty()).isTrue();
            assertThrows(IndexOutOfBoundsException.class, () -> bigArray.get(0));
            assertThrows(IndexOutOfBoundsException.class, () -> bigArray.get(1));
            assertThrows(IndexOutOfBoundsException.class, () -> bigArray.get(Long.MAX_VALUE));

            bigArray.append("hello".getBytes());
            assertThat(bigArray.getTailIndex()).isEqualTo(0L);
            assertThat(bigArray.getHeadIndex()).isEqualTo(1L);
            assertThat(bigArray.size()).isEqualTo(1L);
            assertThat(bigArray.isEmpty()).isFalse();
            assertThat(new String(bigArray.get(0))).isEqualTo("hello");

            bigArray.flush();

            bigArray.append("world".getBytes());
            assertThat(bigArray.getTailIndex()).isEqualTo(0L);
            assertThat(bigArray.getHeadIndex()).isEqualTo(2L);
            assertThat(bigArray.size()).isEqualTo(2L);
            assertThat(bigArray.isEmpty()).isFalse();
            assertThat(new String(bigArray.get(0))).isEqualTo("hello");
            assertThat(new String(bigArray.get(1))).isEqualTo("world");

            bigArray.removeAll();
        }
    }

    @Test
    public void removeBeforeIndex_big() {
        BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME);
        assertThat(bigArray).isNotNull();

        int loop = 5000_000;
        for (int i = 0; i < loop; i++) {
            bigArray.append(strBytesOf(i));
        }

        int half = loop / 2;
        bigArray.removeBeforeIndex(half);
        assertThat(half).isEqualTo(bigArray.getTailIndex());
        assertThat(half).isEqualTo(bigArray.size());
        assertThat(new String(bigArray.get(half))).isEqualTo(half + "");
        assertThat(new String(bigArray.get(half + 1))).isEqualTo(half + 1 + "");
        assertThrows(IndexOutOfBoundsException.class, () -> bigArray.get(half - 1));

        long last = loop - 1;
        bigArray.removeBeforeIndex(last);
        assertThat(last).isEqualTo(bigArray.getTailIndex());
        assertThat(1).isEqualTo(bigArray.size());
        assertThat(new String(bigArray.get(last))).isEqualTo(last + "");
        assertThrows(IndexOutOfBoundsException.class, () -> bigArray.get(last - 1));
    }

    @Test
    public void removeBefore_big() {
        BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME);
        assertThat(bigArray).isNotNull();

        int loop = 5000_000;
        for (int i = 0; i < loop; i++) {
            bigArray.append(strBytesOf(i));
        }

        int half = loop / 2;
        long halfTimestamp = bigArray.getTimestamp(half);
        bigArray.removeBefore(halfTimestamp);
        long tail = bigArray.getTailIndex();
        assertThat(tail > 0).isTrue();
    }

    @Test
    public void simple_ops_big() {
        int loop = 1000_000;

        try (BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME)) {
            assertThat(bigArray).isNotNull();
            long lastAppendTime = System.currentTimeMillis();
            for (int i = 0; i < loop; i++) {
                bigArray.append(strBytesOf(i));
                assertThat(bigArray.getTailIndex()).isEqualTo(0L);
                assertThat(bigArray.getHeadIndex()).isEqualTo(i + 1L);
                assertThat(bigArray.size()).isEqualTo(i + 1L);
                assertThat(bigArray.isEmpty()).isFalse();
                long currentTime = System.currentTimeMillis();
                long justAppendTime = bigArray.getTimestamp(i);
                assertThat(justAppendTime <= currentTime).isTrue();
                assertThat(justAppendTime >= lastAppendTime).isTrue();
                lastAppendTime = justAppendTime;
            }
            assertThrows(IndexOutOfBoundsException.class, () -> bigArray.get(loop));

            assertThat(bigArray.getTailIndex()).isEqualTo(0L);
            assertThat(bigArray.getHeadIndex()).isEqualTo(loop);
            assertThat(bigArray.size()).isEqualTo(loop);
            assertThat(bigArray.isEmpty()).isFalse();
        }

        try (BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME)) {
            assertThat(bigArray.getTailIndex()).isEqualTo(0L);
            assertThat(bigArray.getHeadIndex()).isEqualTo(loop);
            assertThat(bigArray.size()).isEqualTo(loop);
            assertThat(bigArray.isEmpty()).isFalse();

            long lastAppendTime = System.currentTimeMillis();
            for (long i = 0; i < 10; i++) {
                bigArray.append(strBytesOf(i));
                long currentTime = System.currentTimeMillis();
                long justAppendTime = bigArray.getTimestamp(loop + i);
                assertThat(justAppendTime <= currentTime).isTrue();
                assertThat(justAppendTime >= lastAppendTime).isTrue();
                lastAppendTime = justAppendTime;
                assertThat(new String(bigArray.get(loop + i))).isEqualTo(i + "");
            }
            assertThat(bigArray.getTailIndex()).isEqualTo(0L);
            assertThat(bigArray.getHeadIndex()).isEqualTo(loop + 10);
            assertThat(bigArray.size()).isEqualTo(loop + 10);
            assertThat(bigArray.isEmpty()).isFalse();
        }
    }

    @Test
    public void simple_access_timed() {
        BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME);
        assertThat(bigArray).isNotNull();
        int loop = 1000_000;
        byte[][] data = fill(new byte[loop][], TestingData::strBytesOf);

        TimeIt.timeIt(() -> {
            for (int i = 0; i < loop; i++) {
                bigArray.append(data[i]);
            }
        }).onDone(millis -> log.debug().log("Time used to sequentially append %d items : %d millis", loop, millis));

        TimeIt.timeIt(() -> {
            for (int i = 0; i < loop; i++) {
                assertThat(bigArray.get(i)).isEqualTo(data[i]);
            }
        }).onDone(millis -> log.debug().log("Time used to sequentially read %d items : %d millis", loop, millis));

        List<Long> list = TimeIt.timeIt(() ->
            random.randomPermutation(loop)
        ).onDone((result, millis) -> log.debug().log("Time used to shuffle %d items : %d millis", loop, millis));

        TimeIt.timeIt(() -> {
            for (long i : list) {
                assertThat(bigArray.get(i)).isEqualTo(data[(int) i]);
            }
        }).onDone(millis -> log.debug().log("Time used to randomly read %d items : %d millis", loop, millis));
    }

    @Test
    public void findClosestIndex_simple() {
        BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME);
        assertThat(bigArray).isNotNull();
        assertThat(bigArray.findClosestIndex(System.currentTimeMillis())).isEqualTo(-1);

        int loop = 2000_000;
        long begin = System.currentTimeMillis();
        TestingBasics.waitFor(100);
        for (int i = 0; i < loop; i++) {
            bigArray.append(strBytesOf(i));
        }
        TestingBasics.waitFor(100);
        long end = System.currentTimeMillis();

        long midTs = (end + begin) / 2;

        assertThat(0L).isEqualTo(bigArray.findClosestIndex(begin));
        assertThat(loop - 1).isEqualTo(bigArray.findClosestIndex(end));

        long midIndex = bigArray.findClosestIndex(midTs);
        assertThat(0L < midIndex).isTrue();
        assertThat(loop - 1 > midIndex).isTrue();

        long closestTime = bigArray.getTimestamp(midIndex);
        long closestTimeBefore = bigArray.getTimestamp(midIndex - 1);
        long closestTimeAfter = bigArray.getTimestamp(midIndex + 1);
        assertThat(closestTimeBefore <= closestTime).isTrue();
        assertThat(closestTimeAfter >= closestTime).isTrue();
    }

    @Test
    public void getBackFileSize_simple() {
        BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME);
        assertThat(bigArray).isNotNull();
        assertThat(bigArray.getBackFileSize()).isEqualTo(0);

        bigArray.append("hello".getBytes());
        assertThat(bigArray.getBackFileSize()).isEqualTo(INDEX_PAGE_SIZE + bigArray.getDataPageSize());

        long loop = 3000_000;
        byte[] data = random.randomBytes(256);
        for (long i = 0; i < loop; i++) {
            bigArray.append(data);
        }
        assertThat(bigArray.getBackFileSize()).isEqualTo(INDEX_PAGE_SIZE * 23 + bigArray.getDataPageSize() * 6L);

        bigArray.removeBeforeIndex(loop / 2);
        TestingBasics.waitFor(100);
        assertThat(bigArray.getBackFileSize()).isEqualTo(INDEX_PAGE_SIZE * 12 + bigArray.getDataPageSize() * 4L);

        bigArray.removeAll();
        assertThat(bigArray.getBackFileSize()).isEqualTo(0);
    }

    @Test
    public void limitBackFileSize_simple() {
        BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME);
        assertThat(bigArray).isNotNull();
        assertThat(bigArray.getBackFileSize()).isEqualTo(0);

        bigArray.append("hello".getBytes());
        assertThat(bigArray.getBackFileSize()).isEqualTo(INDEX_PAGE_SIZE + bigArray.getDataPageSize());

        bigArray.limitBackFileSize(bigArray.getDataPageSize()); // no effect
        assertThat(bigArray.getBackFileSize()).isEqualTo(INDEX_PAGE_SIZE + bigArray.getDataPageSize());

        long loop = 3000_000;
        byte[] data = random.randomBytes(256);
        for (long i = 0; i < loop; i++) {
            bigArray.append(data);
        }

        bigArray.limitBackFileSize(INDEX_PAGE_SIZE * 12 + bigArray.getDataPageSize() * 3L);
        assertThat(bigArray.getBackFileSize() <= INDEX_PAGE_SIZE * 12 + bigArray.getDataPageSize() * 3L).isTrue();
        assertThat(bigArray.getBackFileSize() > INDEX_PAGE_SIZE * 11 + bigArray.getDataPageSize() * 2L).isTrue();
        long lastTailIndex = bigArray.getTailIndex();
        assertThat(lastTailIndex > 0).isTrue();
        assertThat(bigArray.getHeadIndex()).isEqualTo(loop + 1);

        bigArray.limitBackFileSize(INDEX_PAGE_SIZE * 8 + bigArray.getDataPageSize() * 2L);
        assertThat(bigArray.getBackFileSize() <= INDEX_PAGE_SIZE * 8 + bigArray.getDataPageSize() * 2L).isTrue();
        assertThat(bigArray.getBackFileSize() > INDEX_PAGE_SIZE * 7 + bigArray.getDataPageSize()).isTrue();
        assertThat(bigArray.getTailIndex() > lastTailIndex).isTrue();
        lastTailIndex = bigArray.getTailIndex();
        assertThat(bigArray.getHeadIndex()).isEqualTo(loop + 1);

        bigArray.limitBackFileSize(INDEX_PAGE_SIZE * 4 + bigArray.getDataPageSize());
        assertThat(bigArray.getBackFileSize()).isEqualTo(INDEX_PAGE_SIZE * 3 + bigArray.getDataPageSize());
        assertThat(bigArray.getTailIndex() > lastTailIndex).isTrue();
        lastTailIndex = bigArray.getTailIndex();
        assertThat(bigArray.getHeadIndex()).isEqualTo(loop + 1);

        bigArray.append("world".getBytes());
        assertThat(bigArray.getTailIndex()).isEqualTo(lastTailIndex);
        assertThat(bigArray.getHeadIndex()).isEqualTo(loop + 2);
    }

    @Test
    public void getItemLength_simple() {
        BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME);
        assertThat(bigArray).isNotNull();

        for (int i = 1; i <= 100; i++) {
            bigArray.append(random.randomBytes(i));
        }

        for (int i = 1; i <= 100; i++) {
            int length = bigArray.getItemLength(i - 1);
            assertThat(length).isEqualTo(i);
        }
    }

    @Test
    @SuppressWarnings("resource")
    public void invalid_data_page_size() {
        assertThrows(IllegalArgumentException.class, () -> {
            new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME, MINIMUM_DATA_PAGE_SIZE - 1);
        });
    }

    @Test
    @Tag("stress")
    public void getBackFileSize_minimum_data_page_size() {
        TestSize.assumeTestSize(TestSize.STRESS);

        BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME, MINIMUM_DATA_PAGE_SIZE);
        byte[] data = random.randomBytes(MINIMUM_DATA_PAGE_SIZE / (1024 * 1024));

        assertThat(bigArray).isNotNull();
        for (int i = 0; i < 1024 * 1024; i++) {
            bigArray.append(data);
        }
        assertThat(bigArray.getBackFileSize()).isEqualTo(64 * 1024 * 1024);

        for (int i = 0; i < 1024 * 1024 * 10; i++) {
            bigArray.append(data);
        }
        assertThat(bigArray.getBackFileSize()).isEqualTo(11 * 64 * 1024 * 1024);

        bigArray.removeBeforeIndex(1024 * 1024);
        TestingBasics.waitFor(100);
        assertThat(bigArray.getBackFileSize()).isEqualTo(10 * 64 * 1024 * 1024);

        bigArray.removeBeforeIndex(1024 * 1024 * 2);
        TestingBasics.waitFor(100);
        assertThat(bigArray.getBackFileSize()).isEqualTo(9 * 64 * 1024 * 1024);
    }

    @Test
    @Tag("stress")
    public void data_corruption() {
        TestSize.assumeTestSize(TestSize.STRESS);

        long totalSize = 2L * 1024 * 1024 * 1024;
        int dataSize = 4096;
        int pageSize = DEFAULT_DATA_PAGE_SIZE;
        long limitSize = 500 * 1024 * 1024;
        long dataNum = totalSize / dataSize;

        byte[] data = new byte[dataSize];
        Arrays.fill(data, (byte) 'a');

        BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME, pageSize);
        assertThat(bigArray).isNotNull();

        newExecutor().scheduleAtFixedRate(() -> bigArray.limitBackFileSize(limitSize), 100, 100, TimeUnit.MILLISECONDS);

        log.debug().log("dataNum: %s", dataNum);
        for (int i = 0; i < dataNum; ++i) {
            bigArray.append(data);
        }

        int count = 0;
        for (long i = bigArray.getTailIndex(); i < bigArray.getHeadIndex(); ++i) {
            byte[] bytes;
            try {
                bytes = bigArray.get(i);
            } catch (IndexOutOfBoundsException e) {
                log.debug().log("%d is reset to %d", i, bigArray.getTailIndex());
                i = bigArray.getTailIndex();
                continue; // reset
            }
            assertThat(bytes).isEqualTo(data);
            ++count;
        }
        log.debug().log("count=%d", count);
    }

    @Test
    public void mod_simple() {
        assertThat(BigArrayImpl.mod(1024, 1)).isEqualTo(0);
        assertThat(BigArrayImpl.mod(1024, 2)).isEqualTo(0);
        assertThat(BigArrayImpl.mod(1024, 5)).isEqualTo(0);
        assertThat(BigArrayImpl.mod(1024, 10)).isEqualTo(0);

        assertThat(BigArrayImpl.mod(1025, 10)).isEqualTo(1);
        assertThat(BigArrayImpl.mod(1027, 10)).isEqualTo(3);

        assertThat(BigArrayImpl.mod(0, 10)).isEqualTo(0);

        assertThat(BigArrayImpl.mod(Long.MAX_VALUE, 10)).isEqualTo(1023);
        for (int i = 0; i <= 1023; i++) {
            assertThat(BigArrayImpl.mod(Long.MAX_VALUE - i, 10)).isEqualTo(1023 - i);
        }
    }

    @Test
    public void mul_simple() {
        for (int i = 0; i <= 60; i++) {
            assertThat(BigArrayImpl.mul(Integer.MAX_VALUE, i)).isEqualTo(Integer.MAX_VALUE * (long) Math.pow(2, i));
        }
    }

    @Test
    public void div_simple() {
        for (int i = 0; i <= 60; i++) {
            assertThat(BigArrayImpl.div(Long.MAX_VALUE, i)).isEqualTo(Long.MAX_VALUE / (long) Math.pow(2, i));
        }
    }

    private static ScheduledExecutorService newExecutor() {
        return CLOSE_ALL.addCloseablePerTest(Executors.newScheduledThreadPool(1));
    }
}
