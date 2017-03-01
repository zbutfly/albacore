package net.butfly.albacore.io;

import static net.butfly.albacore.utils.Exceptions.unwrap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import net.butfly.albacore.utils.Utils;

public final class Its extends Utils {
	public static <V> Spliterator<V> wrap(Spliterator<V> it) {
		return new Spliterator<V>() {
			@Override
			public boolean tryAdvance(Consumer<? super V> action) {
				return it.tryAdvance(action);
			}

			@Override
			public Spliterator<V> trySplit() {
				return it.trySplit();
			}

			@Override
			public long estimateSize() {
				return it.estimateSize();
			}

			@Override
			public int characteristics() {
				return it.characteristics();
			}
		};
	}

	public static <V> Iterator<V> it(Supplier<V> get, Supplier<Boolean> ending) {
		return new Iterator<V>() {
			@Override
			public boolean hasNext() {
				return !ending.get();
			}

			@Override
			public V next() {
				return get.get();
			}
		};
	}

	public static <V> Iterator<V> lock(Iterator<V> it, ReentrantReadWriteLock lock) {
		return new Iterator<V>() {
			@Override
			public boolean hasNext() {
				if (null != lock) lock.writeLock().lock();
				try {
					return it.hasNext();
				} finally {
					if (null != lock) lock.writeLock().unlock();
				}
			}

			@Override
			public V next() {
				if (null != lock) lock.writeLock().lock();
				try {
					return it.next();
				} finally {
					if (null != lock) lock.writeLock().unlock();
				}
			}
		};
	}

	public static <V> Iterator<V> it(Spliterator<V> t) {
		// return Spliterators.iterator(t);
		return new Iterator<V>() {
			@Override
			public boolean hasNext() {
				return t.estimateSize() > 0;
			}

			@Override
			public V next() {
				AtomicReference<V> ref = new AtomicReference<>();
				if (!t.tryAdvance(v -> ref.set(v))) return null;
				return ref.get();
			}
		};
	}

	public static <V> Iterator<V> loop(Iterable<V> itbl) {
		return new Iterator<V>() {
			Iterator<V> it = itbl.iterator();

			@Override
			public boolean hasNext() {
				return true;
			}

			@Override
			public V next() {
				if (!it.hasNext()) it = itbl.iterator();
				return it.next();
			}
		};
	}

	public static <V> void splitRun(Spliterator<V> origin, long max, Consumer<Spliterator<V>> using) {
		List<Future<?>> fs = new ArrayList<>();
		while (origin.estimateSize() > max) {
			Spliterator<V> split = origin.trySplit();
			if (null != split) fs.add(IO.listenRun(() -> splitRun(split, max, using)));
		}
		if (origin.estimateSize() > 0) using.accept(origin);
		for (Future<?> f : fs) {
			try {
				f.get();
			} catch (InterruptedException e) {} catch (ExecutionException e) {
				StreamExecutor.logger.error("Subtask error", unwrap(e));
			}
		}
	}

	public static <V, R> Spliterator<R> splitMap(Spliterator<V> origin, long maxSize, Function<Spliterator<V>, Spliterator<R>> using) {
		List<Future<Spliterator<R>>> fs = new ArrayList<>();
		while (origin.estimateSize() > maxSize) {
			Spliterator<V> split = origin.trySplit();
			if (null != split) fs.add(IO.listen(() -> splitMap(split, maxSize, using)));
		}
		Spliterator<R> v = origin.estimateSize() > 0 ? using.apply(origin) : null;
		List<Spliterator<R>> rs = IO.list(fs, f -> {
			try {
				return f.get();
			} catch (InterruptedException | ExecutionException e) {
				return Spliterators.emptySpliterator();
			}
		});
		if (null != v) rs.add(0, v);
		return new ConcatSpliterator<>(rs);
	}

	private static class ConcatSpliterator<V> implements Spliterator<V> {
		private final Iterator<Spliterator<V>> it;
		private final AtomicLong estimateSize;
		private Spliterator<V> curr, next;

		private ConcatSpliterator(Iterable<Spliterator<V>> splits) {
			super();
			it = splits.iterator();
			curr = it.next();
			next = it.hasNext() ? it.next() : null;
			long s = 0;
			for (Spliterator<V> it : splits) {
				long s0 = it.estimateSize();
				if (s0 == Long.MAX_VALUE || (s += s0) < -1) {
					s = Long.MAX_VALUE;
					break;
				}
			}
			estimateSize = s < Long.MAX_VALUE ? new AtomicLong(s) : null;
		}

		@Override
		public boolean tryAdvance(Consumer<? super V> using) {
			boolean used;
			while (!(used = curr.tryAdvance(using))) {
				if (null != next) {
					curr = next;
					next = it.hasNext() ? it.next() : null;
				} else return false;
			}
			if (used && estimateSize != null) estimateSize.decrementAndGet();
			return used;
		}

		@Override
		public Spliterator<V> trySplit() {
			if (null != next) {
				Spliterator<V> s = next;
				next = it.hasNext() ? it.next() : null;
				return s;
			}
			return curr.trySplit();
		}

		@Override
		public long estimateSize() {
			return estimateSize.get();
		}

		@Override
		public int characteristics() {
			return curr.characteristics();
		}
	}
}
