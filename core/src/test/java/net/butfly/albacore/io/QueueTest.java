package net.butfly.albacore.io;

import org.slf4j.event.Level;

import net.butfly.albacore.io.impl.LoggerOutput;
import net.butfly.albacore.io.impl.RandomStringInput;
import net.butfly.albacore.io.pump.Pump;

public class QueueTest {
	public static void main(String... args) {
		Pump.run(RandomStringInput.INSTANCE.pump(new LoggerOutput(Level.WARN), 3, () -> false));
	}
}
