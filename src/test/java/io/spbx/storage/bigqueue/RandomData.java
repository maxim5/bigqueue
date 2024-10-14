package io.spbx.storage.bigqueue;

import io.spbx.util.base.BasicRandom.RandomStrings;
import io.spbx.util.logging.Logger;
import io.spbx.util.testing.Seeder;
import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

@ThreadSafe
public class RandomData {
    private static final Logger log = Logger.forEnclosingClass();
    private final SeedSupplier seeder;

    RandomData(@NotNull SeedSupplier seeder) {
        this.seeder = seeder;
    }

    // To be used in worker threads.
    public static @NotNull RandomData seededForEachCallSite() {
        Seeder seeder = new Seeder();
        Class<?> target = RandomData.class;
        return new RandomData(() -> seeder.nextSeedForCallerOf(target));
    }

    // To be used in the main thread.
    public static @NotNull RandomData withSharedSeed() {
        Random random = new Random(0);
        return new RandomData(random::nextInt);
    }

    public @NotNull String randomString(int len) {
        long seed = seeder.seed();
        String result = RandomStrings.of(seed).next(RandomStrings.ASCII_DIGITS_LOWER_CASE, len);
        if (log.ultra().isEnabled()) {
            log.ultra().log("randomString(%d): seed=%d: %s", len, seed, result);
        } else {
            log.trace().log("randomString(%d): seed=%d", len, seed);
        }
        return result;
    }

    public byte @NotNull[] randomBytes(int length) {
        return randomString(length).getBytes();
    }

    public @NotNull List<Long> randomPermutation(int num) {
        long seed = seeder.seed();
        List<Long> result = new ArrayList<>(num);
        for (long i = 0; i < num; i++) {
            result.add(i);
        }
        Collections.shuffle(result, new Random(seed));
        if (log.ultra().isEnabled()) {
            log.ultra().log("randomPermutation(%d): seed=%d: %s", num, seed, result);
        } else {
            log.trace().log("randomPermutation(%d): seed=%d", num, seed);
        }
        return result;
    }

    private interface SeedSupplier {
        long seed();
    }
}
