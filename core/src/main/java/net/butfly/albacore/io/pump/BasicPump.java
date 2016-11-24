package net.butfly.albacore.io.pump;

import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.utils.Reflections;

public class BasicPump<V> extends PumpImpl<V> {
	private static final long serialVersionUID = 663917114528791086L;

	public BasicPump(Q<?, V> source, Q<V, ?> destination, int parallelism) {
		super(source.name() + "-to-" + destination.name(), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", source, destination);
		pumping(() -> source.empty(), () -> destination.enqueue(stats(source.dequeue(batchSize))));
	}
}
