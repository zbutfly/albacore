package net.butfly.albacore.io.queue;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.io.Streams;
import net.butfly.albacore.utils.parallel.Concurrents;

public abstract class QueueImpl<V> implements Queue<V> {
	private final String name;
	private final AtomicLong capacity;

	protected QueueImpl(long capacity) {
		super();
		this.name = Queue.super.name();
		this.capacity = new AtomicLong(capacity);
	}

	protected QueueImpl(String name, long capacity) {
		super();
		this.name = name;
		this.capacity = new AtomicLong(capacity);
	}

	protected abstract boolean enqueue(V item);

	@Override
	public long enqueue(Stream<V> items) {
		if (!Concurrents.waitSleep(() -> full())) return 0;
		AtomicLong c = new AtomicLong(0);
		Streams.of(items).forEach(t -> {
			if (enqueue(t)) c.incrementAndGet();
		});
		return c.get();
	}

	protected V dequeue() {
		AtomicReference<V> ref = new AtomicReference<>();
		dequeue(s -> {
			ref.lazySet(s.findFirst().orElse(null));
			return 1L;
		}, 1);
		return ref.get();
	}

	@Override
	public long dequeue(Function<Stream<V>, Long> using, long batchSize) {
		return using.apply(Streams.of(() -> dequeue(), batchSize, () -> empty() && opened()));
	}

	@Override
	public final String name() {
		return name;
	}

	@Override
	public final long capacity() {
		return capacity.get();
	}

	@Override
	public String toString() {
		return name();
	}

}
