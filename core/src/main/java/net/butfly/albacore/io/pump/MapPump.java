package net.butfly.albacore.io.pump;

import net.butfly.albacore.io.queue.Q;

import java.util.List;

import net.butfly.albacore.io.queue.MapQ;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.Reflections;

@Deprecated
public class MapPump extends PumpImpl {
	private static final long serialVersionUID = 1781793229310906740L;

	public <K, V> MapPump(Q<?, V> source, MapQ<K, V, ?> destination, Converter<V, K> keying, int parallelism) {
		super(source.name() + ">" + destination.name(), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", source, destination);
		pumping(() -> source.empty(), () -> {
			if (source.opened()) {
				List<V> l = Collections.cleanNull(source.dequeue(batchSize));
				if (l.size() > 0) {
					stats(l);
					destination.enqueue(keying, l);
				}
			}
		});
	}
}
