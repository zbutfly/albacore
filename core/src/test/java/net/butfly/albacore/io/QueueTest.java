package net.butfly.albacore.io;

import net.butfly.albacore.io.impl.LoggerOutput;
import net.butfly.albacore.io.impl.RandomStringInput;
import net.butfly.albacore.utils.async.Concurrents;

public class QueueTest {
	public static void main(String... args) {
		DirectPump<String> p = RandomStringInput.INSTANCE.pump(new LoggerOutput(), 3, 2);
		p.interval(() -> Concurrents.waitSleep(3000));
		p.start().waiting();
	}
}
