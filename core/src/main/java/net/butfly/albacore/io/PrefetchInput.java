package net.butfly.albacore.io;

import java.util.function.Consumer;
import java.util.stream.Stream;

import net.butfly.albacore.io.Wrapper.WrapInput;
import net.butfly.albacore.io.queue.Queue;

class PrefetchInput<V> extends WrapInput<V> {
	private final Queue<V> pool;
	private final OpenableThread fetcher;

	PrefetchInput(Input<V> base, Queue<V> pool, long fetchSize) {
		super(base);
		this.pool = pool;
		fetcher = new OpenableThread(() -> {
			while (opened() && !base.empty())
				base.dequeue(pool::enqueue, fetchSize);
		}, base.name() + "PrefetcherThread");
	}

	@Override
	public void dequeue(Consumer<Stream<V>> using, long batchSize) {
		pool.dequeue(using, batchSize);
	}

	@Override
	public void close() {
		fetcher.close();
		super.close();
	}
}
