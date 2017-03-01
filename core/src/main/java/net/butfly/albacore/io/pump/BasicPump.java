package net.butfly.albacore.io.pump;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.utils.Reflections;

public class BasicPump<V> extends PumpImpl<V, BasicPump<V>> {
	public BasicPump(Input<V> input, int parallelism, Output<V> output) {
		super(input.name() + ">" + output.name(), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", input, output);
		depend(input, output);
		long forceTrace = batchSize / parallelism;
		pumping(() -> input.empty(), () -> {
			if (opened() && input.opened() && output.opened()) input.dequeue(s -> {
				long c = output.enqueue(stats(s));
				if (c > 0 && c < forceTrace) traceForce("Current process: [" + c + "]");
			}, batchSize);
		});
	}
}
