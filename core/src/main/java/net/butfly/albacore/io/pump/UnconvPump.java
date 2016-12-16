package net.butfly.albacore.io.pump;

import java.util.List;

import net.butfly.albacore.io.queue.Q;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Reflections;

public class UnconvPump<V> extends PumpImpl<V> {
	private static final long serialVersionUID = 663917114528791086L;

	public <I1> UnconvPump(Q<?, V> source, Q<I1, ?> destination, int parallelism, Converter<List<V>, List<I1>> unconv) {
		super(source.name() + "-to-" + destination.name(), parallelism);
		Reflections.noneNull("Pump source/destination should not be null", source, destination);
		pumping(() -> source.empty(), () -> {
			List<V> l = source.dequeue(batchSize);
			stats(l);
			destination.enqueue(unconv.apply(l));
		});
	}
}
