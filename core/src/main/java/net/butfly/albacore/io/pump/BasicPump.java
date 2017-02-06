package net.butfly.albacore.io.pump;

import java.util.stream.Stream;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.utils.Reflections;

public class BasicPump<V> extends PumpImpl<V> {
	public BasicPump(Input<V> input, int parallelism, Output<V> output) {
		super(input.name() + ">" + output.name(), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", input, output);
		depend(input, output);
		pumping(() -> input.empty(), () -> {
			long now, read, stat, writ;
			if (opened() && input.opened() && output.opened()) {
				now = System.currentTimeMillis();
				Stream<V> s = input.dequeue(batchSize);
				read = System.currentTimeMillis() - now;
				now = System.currentTimeMillis();
				s = stats(s);
				stat = System.currentTimeMillis() - now;
				output.enqueue(s);
				writ = System.currentTimeMillis() - now;
				logger().trace("Pump stats: read->" + read + "ms, stat->" + stat + "ms, writ->" + writ + "ms.");
			}
		});
	}
}
