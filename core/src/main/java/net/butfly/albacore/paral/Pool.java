package net.butfly.albacore.paral;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class Pool<V> {
	private final LinkedBlockingQueue<V> pool;
	final Supplier<V> constuctor;
	final Consumer<V> destroyer;

	public Pool(int size, Supplier<V> constuctor) {
		this(size, constuctor, v -> {});
	}

	public Pool(int size, Supplier<V> constuctor, Consumer<V> destroyer) {
		pool = new LinkedBlockingQueue<>(size);
		this.constuctor = constuctor;
		this.destroyer = destroyer;
	}

	public void use(Consumer<V> using) {
		V v = pool.poll();
		if (null == v) v = constuctor.get();
		try {
			using.accept(v);
		} finally {
			if (!pool.offer(v) && v instanceof AutoCloseable) try {
				((AutoCloseable) v).close();
			} catch (Exception e) {}
		}
	}
}
