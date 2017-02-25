package net.butfly.albacore.io;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Supplier;

import net.butfly.albacore.utils.Utils;

public final class Parals extends Utils {
	public static final class Pool<V> {
		private final LinkedBlockingQueue<V> pool;
		private final Supplier<V> constuctor;

		public Pool(int size, Supplier<V> constuctor) {
			pool = new LinkedBlockingQueue<>(size);
			this.constuctor = constuctor;
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

}
