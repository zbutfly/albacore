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
			long cc = c.get();
			if (cc <= 0) Concurrents.waitSleep();
			else if (cc < forceTrace) traceForce(name() + " processing: [" + c + " odd for less than " + forceTrace + "] from [" + input
					.name() + "] to [" + output.name() + "].");
		});
	}
}
