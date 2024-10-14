package io.spbx.storage.bigqueue;

import org.jetbrains.annotations.NotNull;

class TestingData {
    public static byte @NotNull[] strBytesOf(int i) {
        return String.valueOf(i).getBytes();
    }

    public static byte @NotNull[] strBytesOf(long i) {
        return String.valueOf(i).getBytes();
    }
}
