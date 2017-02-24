package net.butfly.albacore.io.pump;

import java.util.stream.Stream;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.utils.Reflections;

public class BasicPump<V> extends PumpImpl<V, BasicPump<V>> {
	public BasicPump(Input<V> input, int parallelism, Output<V> output) {
		super(input.name() + ">" + output.name(), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", input, output);
		depend(input, output);
		pumping(() -> input.empty(), () -> {
			if (opened() && input.opened() && output.opened()) {
				long now = System.currentTimeMillis();
				Stream<V> s = input.dequeue(batchSize);
				logger().error("Time of one reading: " + (System.currentTimeMillis() - now));
				now = System.currentTimeMillis();
				s = stats(s);
				logger().error("Time of one stating: " + (System.currentTimeMillis() - now));
				now = System.currentTimeMillis();
				output.enqueue(s);
				logger().error("Time of one writing: " + (System.currentTimeMillis() - now));
			}
		});
	}
}
