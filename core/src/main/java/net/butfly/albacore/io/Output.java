package net.butfly.albacore.io;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import net.butfly.albacore.base.Sizable;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.async.Concurrents;

public interface Output<V> extends Openable, Sizable {
	@Override
	default long size() {
		return 0;
	}

	boolean enqueue(V item);

	default long enqueue(Stream<V> items) {
		Stream<V> s = items.filter(t -> t != null);
		if (!Concurrents.waitSleep(() -> full())) return 0;
		AtomicLong c = new AtomicLong(0);
		s.forEach(t -> {
			if (enqueue(t)) c.incrementAndGet();
		});
		return c.get();
	}

	default <V0> Output<V0> prior(Converter<V0, V> conv) {
		return new OutputPriorHandler<>(this, conv).proxy(Output.class);
	}

	default <V0> Output<V0> priors(Converter<List<V0>, List<V>> conv, int parallelism) {
		return new OutputPriorsHandler<>(this, conv, parallelism).proxy(Output.class);
	}
}
