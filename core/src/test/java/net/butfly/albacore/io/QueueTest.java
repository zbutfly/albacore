package net.butfly.albacore.io;

import org.slf4j.event.Level;

import net.butfly.albacore.io.impl.LoggerOutput;
import net.butfly.albacore.io.impl.RandomStringInput;

public class QueueTest {
	public static void main(String... args) {
		RandomStringInput.INSTANCE.pump(new LoggerOutput(Level.WARN), 3, () -> false).start().waiting().stop();
	}
}
