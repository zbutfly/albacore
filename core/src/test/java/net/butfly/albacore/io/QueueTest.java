package net.butfly.albacore.io;

import org.slf4j.event.Level;

import net.butfly.albacore.io.impl.LoggerOutput;
import net.butfly.albacore.io.impl.RandomStringInput;
import net.butfly.albacore.utils.async.Concurrents;

public class QueueTest {
	public static void main(String... args) {
		RandomStringInput.INSTANCE.pump(new LoggerOutput(Level.WARN), 3, 2, () -> false, () -> Concurrents.waitSleep(300)).start()
				.waiting().stop();
	}
}
