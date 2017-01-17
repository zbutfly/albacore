package net.butfly.albacore.io.pump;

import java.util.List;

import com.google.common.base.Joiner;

import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.Reflections;

public class FanoutPump extends PumpImpl {
	private static final long serialVersionUID = -9048673309470370198L;

	public <V> FanoutPump(Q<?, V> source, int parallelism, List<? extends Q<V, ?>> destinations) {
		super(source.name() + "-to-" + Joiner.on('-').join(Collections.transform(destinations, d -> d.name())), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", source);
		Reflections.noneNull("Pump source/destination should not be null", destinations);
		sources(source);
		dests(destinations);
		pumping(() -> source.empty(), () -> {
			List<V> l = source.dequeue(batchSize);
			if (l.size() > 0) {
				stats(l);
				for (Q<V, ?> q : destinations)
					q.enqueue(l);
			}
		});
	}
}
