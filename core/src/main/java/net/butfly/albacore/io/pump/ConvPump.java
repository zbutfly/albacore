package net.butfly.albacore.io.pump;

import java.util.List;

import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Reflections;

public class ConvPump extends PumpImpl {
	private static final long serialVersionUID = 663917114528791086L;

	public <O1, V> ConvPump(Q<?, O1> source, Q<V, ?> destination, int parallelism, Converter<List<O1>, List<V>> conv, boolean statsBefore) {
		super(source.name() + "-to-" + destination.name(), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", source, destination);
		pumping(() -> source.empty(), () -> {
			List<O1> ll = source.dequeue(batchSize);
			if (ll.size() > 0) {
				if (statsBefore) stats(ll);
				List<V> l = conv.apply(ll);
				if (!statsBefore) stats(l);
				destination.enqueue(l);
			}
		});
	}
}
