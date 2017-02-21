package net.butfly.albacore.io;

import org.slf4j.event.Level;

import net.butfly.albacore.io.pump.Pump;
import net.butfly.albacore.io.std.LoggerOutput;
import net.butfly.albacore.io.std.RandomStringInput;

public class QueueTest {
	public static void main(String... args) {
		try (Pump<String> p = Pump.pump(RandomStringInput.INSTANCE, 3, new LoggerOutput(Level.WARN))) {
			p.open();
		}
	}
}
