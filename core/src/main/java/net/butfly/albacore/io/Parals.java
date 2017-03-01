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

	public static int calcParallelism(long total, long batch) {
		return total == 0 ? 0 : (int) (((total - 1) / batch) + 1);
	}

	public static long calcBatchSize(long total, int parallelism) {
		return total == 0 ? 0 : (((total - 1) / parallelism) + 1);
	}
}
