package io.spbx.storage.bigqueue;

import io.spbx.storage.bigqueue.page.FakeMappedPageFactory;
import io.spbx.util.base.math.Int128;
import io.spbx.util.base.ops.ByteOps;
import io.spbx.util.testing.ext.CloseAllExtension;
import io.spbx.util.testing.ext.TempDirectoryExtension;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;
import static io.spbx.storage.bigqueue.BigArrayImpl.MINIMUM_DATA_PAGE_SIZE;

@Tag("fast")
@SuppressWarnings("resource")
public class BigArrayImplTest {
    @RegisterExtension private static final TempDirectoryExtension TEMP_DIRECTORY = TempDirectoryExtension.withCleanup();
    @RegisterExtension private static final CloseAllExtension CLOSE_ALL = new CloseAllExtension();
    private static final String ARRAY_NAME = "array_name";

    private final RandomData random = RandomData.withSharedSeed();

    @ParameterizedTest
    @EnumSource(Scenario.class)
    @Tag("slow")
    public void append_then_get_simple(Scenario scenario) {
        setupFactoryMockIfNecessary(scenario);

        BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME, MINIMUM_DATA_PAGE_SIZE);

        assertThat(bigArray.append(Int128.MAX_VALUE.toByteArray())).isEqualTo(0);
        assertThat(bigArray.appendLong(Long.MAX_VALUE)).isEqualTo(1);
        assertThat(bigArray.appendInt(Integer.MAX_VALUE)).isEqualTo(2);
        assertThat(bigArray.appendByte(Byte.MAX_VALUE)).isEqualTo(3);

        assertThat(bigArray.size()).isEqualTo(4);
        assertThat(Int128.fromBits(bigArray.get(0))).isEqualTo(Int128.MAX_VALUE);
        assertThat(bigArray.getLong(1)).isEqualTo(Long.MAX_VALUE);
        assertThat(bigArray.getInt(2)).isEqualTo(Integer.MAX_VALUE);
        assertThat(bigArray.getByte(3)).isEqualTo(Byte.MAX_VALUE);
    }

    @ParameterizedTest
    @EnumSource(Scenario.class)
    @Tag("slow")
    public void append_get_ultimate(Scenario scenario) {
        int loop = 100_000;
        int maxSize = 16 << 10;
        setupFactoryMockIfNecessary(scenario);

        BigArray bigArray = new BigArrayImpl(TEMP_DIRECTORY.currentTempDir(), ARRAY_NAME, MINIMUM_DATA_PAGE_SIZE);
        List<byte[]> list = new ArrayList<>();

        for (int i = 0; i < loop; ++i) {
            int len = random.randomInt(maxSize);
            byte[] data = ByteOps.fill(len, (byte) i);
            list.add(data);

            assertThat(bigArray.append(data)).isEqualTo(i);
            assertThat(bigArray.get(i)).isEqualTo(data);
            assertThat(bigArray.size()).isEqualTo(i + 1);
        }

        for (int i = 0; i < loop; ++i) {
            byte[] data = list.get(i);
            assertThat(bigArray.get(i)).isEqualTo(data);
        }
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

    private static void setupFactoryMockIfNecessary(Scenario scenario) {
        switch (scenario) {
            case FAKE -> CLOSE_ALL.addCloseablePerTest(FakeMappedPageFactory.mockFactory());
            case PROD -> { /* Do nothing */ }
        }
    }

    public enum Scenario {
        FAKE,
        PROD,
    }
}
