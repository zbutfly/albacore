package net.butfly.albacore.io.pump;

import java.util.List;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.utils.Reflections;

public class BasicPump extends PumpImpl {
	private static final long serialVersionUID = 663917114528791086L;

	public <V> BasicPump(Input<V> input, int parallelism, Output<V> output) {
		super(input.name() + ">" + output.name(), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", input, output);
		depend(input, output);
		pumping(() -> input.empty(), () -> {
			if (opened() && input.opened() && output.opened()) {
				List<V> l = input.dequeue(batchSize);
				if (l.size() > 0) {
					stats(l);
					output.enqueue(l);
				}
			}
		});
	}
}
