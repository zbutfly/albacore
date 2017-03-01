package net.butfly.albacore.utils.parallel;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

public class ResPool<R> {
	private final BlockingQueue<R> pool;
	private final Supplier<R> constr;

	public class Res implements AutoCloseable {
		public final R res;

		private Res() {
			R r = pool.poll();
			res = null == r ? constr.get() : r;
		}

		@Override
		public void close() {
			pool.offer(res);
		}
	}

	public ResPool(int cap, Supplier<R> constr) {
		pool = new LinkedBlockingQueue<>(cap);
		this.constr = constr;
	}

	public Res aquire() {
		return new Res();
	}

	public static final ResPool<ReentrantReadWriteLock> RW_LOCKERS = new ResPool<>(Integer.parseInt(System.getProperty(
			"albacore.parallel.pool.number.locker", "100")), () -> new ReentrantReadWriteLock());
	public static final ResPool<ReentrantReadWriteLock> LOCKERS_FAIR = new ResPool<>(Integer.parseInt(System.getProperty(
			"albacore.parallel.pool.number.locker.fair", "100")), () -> new ReentrantReadWriteLock(true));
	public static final ResPool<ReentrantLock> FAIR_LOCKERS = new ResPool<>(100, () -> new ReentrantLock(true));
}
