package io.spbx.storage.bigqueue.tutorial;

import io.spbx.storage.bigqueue.BigQueue;
import io.spbx.storage.bigqueue.BigQueueImpl;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A tutorial to show the basic API usage of the {@link BigQueue}.
 */
public class BigQueueTutorial {
	private static void runDemo(@NotNull Path path) {
        try (BigQueue bigQueue = new BigQueueImpl(path, "demo")) {
			// Make sure the queue is empty
			assert bigQueue.isEmpty();
			assert bigQueue.dequeue() == null;
			assert bigQueue.peek() == null;

            // Enqueue some items
            for (int i = 0; i < 10; i++) {
                String item = String.valueOf(i);
                bigQueue.enqueue(item.getBytes());
            }
			assert !bigQueue.isEmpty();
			assert bigQueue.size() == 10;

            // Peek the front of the queue
			assert new String(bigQueue.peek()).equals(String.valueOf(0));

            // Dequeue some items
            for (int i = 0; i < 5; i++) {
                String item = new String(bigQueue.dequeue());
				assert item.equals(String.valueOf(i));
            }
			assert !bigQueue.isEmpty();
			assert bigQueue.size() == 5;

            // Dequeue all remaining items
            while (true) {
                byte[] data = bigQueue.dequeue();
                if (data == null) break;
            }
			assert bigQueue.isEmpty();
        }
    }

	public static void main(String[] args) throws IOException {
		Path path = args.length > 0 ? Path.of(args[0]) : Files.createTempDirectory("big-queue-tutorial.");
		System.out.printf("The path for the tutorial: %s%n", path);

		runDemo(path);
	}
}
