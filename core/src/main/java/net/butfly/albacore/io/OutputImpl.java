package net.butfly.albacore.io;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.utils.Streams;
import net.butfly.albacore.utils.parallel.Concurrents;

public abstract class OutputImpl<V> extends Namedly implements Output<V> {
	protected OutputImpl() {
		super();
	}

	protected OutputImpl(String name) {
		super(name);
	}

	protected abstract boolean enqueue(V item);

	@Override
	public final long enqueue(Stream<V> items) {
		if (!Concurrents.waitSleep(() -> full())) return 0;
		AtomicLong c = new AtomicLong(0);
		Streams.of(items).forEach(t -> {
			if (enqueue(t)) c.incrementAndGet();
		});
		return c.get();
	}
}
