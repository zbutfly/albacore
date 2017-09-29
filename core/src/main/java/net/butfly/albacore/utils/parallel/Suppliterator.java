package net.butfly.albacore.utils.parallel;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class Suppliterator<V> implements Spliterator<V> {
	private final int chars;
	private final ResPool<ReentrantLock>.Res lock;
	private final Supplier<V> get;
	private final Supplier<Boolean> ending;
	private long est;

	public Suppliterator(Iterator<V> it) {
		this(() -> it.next(), Long.MAX_VALUE, () -> !it.hasNext());
	}

	public Suppliterator(Iterator<V> it, long size) {
		this(() -> it.next(), size, () -> !it.hasNext());
	}

	public Suppliterator(Supplier<V> get, Supplier<Boolean> ending) {
		this(get, Long.MAX_VALUE, ending);
	}

	public Suppliterator(Supplier<V> get, long size, Supplier<Boolean> ending) {
		super();
		est = size;
		int c = Spliterator.CONCURRENT | Spliterator.IMMUTABLE | Spliterator.ORDERED;
		if (!infinite() && est >= 0) c = c | Spliterator.SIZED | Spliterator.SUBSIZED;
		chars = c;
		this.get = get;
		this.ending = ending;
		this.lock = ResPool.FAIR_LOCKERS.aquire();
	}

	@Override
	public boolean tryAdvance(Consumer<? super V> action) {
		if (!lock.res.tryLock()) return false;
		try {
			boolean next = !ending.get();
			if (!next) est = 0;
			else if (!infinite()) est--;
			next = next && est >= 0;
			if (next) {
				V v = get.get();
				next = v != null;
				if (next) action.accept(v);
			}
			return next;
		} finally {
			lock.res.unlock();
		}
	}

	@Override
	public Spliterator<V> trySplit() {
		if (infinite()) return new Suppliterator<>(get, ending);
		if (est < 2) return null;
		lock.res.lock();
		try {
			long est1 = est / 2;
			est -= est1;
			return new Suppliterator<>(get, est1, ending);
		} finally {
			lock.res.unlock();
		}
	}

	@Override
	public long estimateSize() {
		lock.res.lock();
		try {
			return est;
		} finally {
			lock.res.unlock();
		}
	}

	private boolean infinite() {
		return est == Long.MAX_VALUE;
	}

	@Override
	public int characteristics() {
		return chars;
	}

	@Override
	protected void finalize() throws Throwable {
		lock.close();
	}
}