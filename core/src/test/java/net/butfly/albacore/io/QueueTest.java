package net.butfly.albacore.io;

import org.slf4j.event.Level;

import net.butfly.albacore.io.ext.LoggerOutput;
import net.butfly.albacore.io.ext.RandomStringInput;
import net.butfly.albacore.io.pump.Pump;

public class QueueTest {
	public static void main(String... args) {
		try (Pump<String> p = Pump.pump(RandomStringInput.INSTANCE, 3, new LoggerOutput(Level.WARN))) {
			p.open();
		}
	}
}
