package io.spbx.storage.bigqueue;

import io.spbx.util.logging.Logger;
import org.jetbrains.annotations.NotNull;

import static io.spbx.util.func.ScopeFunctions.also;

public class Timeouts {
    private static final Logger log = Logger.forEnclosingClass();
    private static final Tier tier = also(detectSystemTier(), val -> log.debug().log("Detected System Tier: %s", val));

    public static int timeout(int fast, int slow) {
        return tier == Tier.SLOW ? slow : fast;
    }

    public static int timeout(int fast, int medium, int slow) {
        return tier == Tier.SLOW ? slow : tier == Tier.MEDIUM ? medium : fast;
    }

    enum Tier {
        FAST,
        MEDIUM,
        SLOW,
    }

    private static @NotNull Tier detectSystemTier() {
        if (Env.isCI()) {
            return Tier.SLOW;   // assume low-level CI servers
        }

        if (Env.cpuCount() < 8) {
            return Tier.SLOW;
        } else if (Env.cpuCount() == 8) {
            return Tier.MEDIUM;
        } else {
            return Tier.FAST;
        }
    }

    private static class Env {
        // https://stackoverflow.com/questions/73973332/check-if-were-in-a-github-action-travis-ci-circle-ci-etc-testing-environme
        public static boolean isCI() {
            return System.getenv("GITHUB_ACTIONS") != null ||
                   System.getenv("TRAVIS") != null ||
                   System.getenv("CIRCLECI") != null ||
                   System.getenv("GITLAB_CI") != null;
        }

        public static int cpuCount() {
            return Runtime.getRuntime().availableProcessors();
        }

        public static String cpuArch() {
            return System.getProperty("os.arch");
        }

        public static long freeMemory() {
            return Runtime.getRuntime().freeMemory();
        }
    }
}
