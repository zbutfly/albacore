package net.butfly.albacore.io.pump;

import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.parallel.Concurrents;

public class BasicPump<V> extends PumpImpl<V, BasicPump<V>> {
	public BasicPump(Input<V> input, int parallelism, Output<V> output) {
		super(input.name() + ">" + output.name(), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", input, output);
		depend(input, output);
		long forceTrace = batchSize / parallelism;
		pumping(() -> input.empty(), () -> {
			AtomicLong c = new AtomicLong();
			if (opened()) input.dequeue(s -> {
				c.set(output.enqueue(stats(s)));
			}, batchSize);
			if (c.get() > 0 && c.get() < forceTrace) traceForce("Processing scattered: [" + c + "] to " + output.name());
			if (c.get() == 0) {
				logger().debug("No data, waiting....");
				Concurrents.waitSleep(500);
			}
		});
	}
}
