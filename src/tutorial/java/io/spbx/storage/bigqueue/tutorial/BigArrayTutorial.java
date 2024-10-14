package io.spbx.storage.bigqueue.tutorial;

import io.spbx.storage.bigqueue.BigArray;
import io.spbx.storage.bigqueue.BigArrayImpl;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A tutorial to show the basic API usage of the {@link BigArray}.
 */
public class BigArrayTutorial {
    private static void runDemo(@NotNull Path path) {
        try (BigArray bigArray = new BigArrayImpl(path, "demo")) {
            // Make sure the array is empty
            assert bigArray.isEmpty();
            assert bigArray.getHeadIndex() == 0;
            assert bigArray.getTailIndex() == 0;

            // Append some items into the array
            for (int i = 0; i < 10; i++) {
                String item = String.valueOf(i);
                long index = bigArray.append(item.getBytes());
                assert index == i;
            }
            assert bigArray.size() == 10;
            assert bigArray.getHeadIndex() == 10;
            assert bigArray.getTailIndex() == 0;

            // Randomly read items from the array
            String item0 = new String(bigArray.get(0));
            assert item0.equals(String.valueOf(0));

            String item3 = new String(bigArray.get(3));
            assert item3.equals(String.valueOf(3));

            String item9 = new String(bigArray.get(9));
            assert item9.equals(String.valueOf(9));

            // Clear the array
            bigArray.removeAll();
            assert bigArray.isEmpty();
        }
    }

    public static void main(String[] args) throws IOException {
        Path path = args.length > 0 ? Path.of(args[0]) : Files.createTempDirectory("big-array-tutorial.");
        System.out.printf("The path for the tutorial: %s%n", path);

        runDemo(path);
    }
}
