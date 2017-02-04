package net.butfly.albacore.io.pump;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.utils.Reflections;

public class BasicPump<V> extends PumpImpl<V> {
	private static final long serialVersionUID = 663917114528791086L;

	public BasicPump(Input<V> input, int parallelism, Output<V> output) {
		super(input.name() + ">" + output.name(), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", input, output);
		depend(input, output);
		pumping(() -> input.empty(), () -> {
			// TODO: stats(t);
			if (opened() && input.opened() && output.opened()) output.enqueue(input.dequeue(batchSize));
		});
	}
}
