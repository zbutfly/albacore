package net.butfly.albacore.io.pump;

import java.util.List;

import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Reflections;

public class ConvPump<V> extends PumpImpl<V> {
	private static final long serialVersionUID = 663917114528791086L;

	public <O1> ConvPump(Q<?, O1> source, Q<V, ?> destination, int parallelism, Converter<List<O1>, List<V>> conv) {
		super(source.name() + "-to-" + destination.name(), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", source, destination);
		pumping(() -> source.empty(), () -> {
			List<V> l = conv.apply(source.dequeue(batchSize));
			stats(l);
			destination.enqueue(l);
		});
	}
}
