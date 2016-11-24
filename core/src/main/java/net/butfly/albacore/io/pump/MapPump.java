package net.butfly.albacore.io.pump;

import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.io.queue.MapQ;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Reflections;

public class MapPump<K, V> extends PumpImpl<V> {
	private static final long serialVersionUID = 1781793229310906740L;

	public MapPump(Q<?, V> source, MapQ<K, V, ?> destination, Converter<V, K> keying, int parallelism) {
		super(source.name() + "-to-" + destination.name(), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", source, destination);
		pumping(() -> source.empty(), () -> destination.enqueue(keying, stats(source.dequeue(batchSize))));
	}
}
