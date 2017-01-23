package net.butfly.albacore.io.pump;

import java.util.List;

import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.utils.Reflections;

public class BasicPump extends PumpImpl {
	private static final long serialVersionUID = 663917114528791086L;

	public <V> BasicPump(Q<?, V> source, Q<V, ?> destination, int parallelism) {
		super(source.name() + ">" + destination.name(), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", source, destination);
		depend(source, destination);
		pumping(() -> source.empty(), () -> {
			if (source.opened()) {
				List<V> l = source.dequeue(batchSize);
				if (l.size() > 0) {
					stats(l);
					destination.enqueue(l);
				}
			}
		});
	}
}
