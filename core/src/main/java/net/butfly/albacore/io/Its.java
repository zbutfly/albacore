package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
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

	public static <V> Iterator<V> it(Supplier<V> get, Supplier<Boolean> end) {
		return new Iterator<V>() {
			@Override
			public boolean hasNext() {
				return !end.get();
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
}
