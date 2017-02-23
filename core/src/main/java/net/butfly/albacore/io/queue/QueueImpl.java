package net.butfly.albacore.io.queue;

import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import net.butfly.albacore.io.Streams;
import net.butfly.albacore.utils.async.Concurrents;

public abstract class QueueImpl<I, O> implements Queue<I, O> {
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

	protected abstract boolean enqueue(I item);

	@Override
	public long enqueue(Stream<I> items) {
		if (!Concurrents.waitSleep(() -> full())) return 0;
		AtomicLong c = new AtomicLong(0);
		Streams.of(items).forEach(t -> {
			if (enqueue(t)) c.incrementAndGet();
		});
		return c.get();
	}

	protected abstract O dequeue();

	@Override
	public Stream<O> dequeue(long batchSize) {
		return Streams.of(Streams.batch(batchSize, () -> dequeue(), () -> empty()));
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
