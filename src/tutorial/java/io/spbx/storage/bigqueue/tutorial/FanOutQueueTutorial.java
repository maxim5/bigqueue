package io.spbx.storage.bigqueue.tutorial;

import io.spbx.storage.bigqueue.FanOutQueue;
import io.spbx.storage.bigqueue.FanOutQueueImpl;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A tutorial to show the basic API usage of the {@link FanOutQueue}.
 */
public class FanOutQueueTutorial {
	private static void runDemo(@NotNull Path path) {
        try (FanOutQueue queue = new FanOutQueueImpl(path, "demo")) {
            // Enqueue some logs
            for (int i = 0; i < 10; i++) {
                String log = "log-" + i;
                queue.enqueue(log.getBytes());
            }

            // Consume the queue with fanoutId1
            String fanoutId1 = "realtime";
            System.out.printf("Output from %s consumer:%n", fanoutId1);
            while (!queue.isEmpty(fanoutId1)) {
                String item = new String(queue.dequeue(fanoutId1));
                System.out.println(item);
            }

			// Consume the queue with fanoutId2
            String fanoutId2 = "offline";
            System.out.printf("Output from %s consumer:%n", fanoutId2);
            while (!queue.isEmpty(fanoutId2)) {
                String item = new String(queue.dequeue(fanoutId2));
                System.out.println(item);
            }
        }
    }

	public static void main(String[] args) throws IOException {
		Path path = args.length > 0 ? Path.of(args[0]) : Files.createTempDirectory("fan-out-tutorial.");
        System.out.printf("The path for the tutorial: %s%n", path);

		runDemo(path);
	}
}
