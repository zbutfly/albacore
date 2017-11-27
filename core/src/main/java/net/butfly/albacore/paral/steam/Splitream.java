package net.butfly.albacore.paral.steam;

import static net.butfly.albacore.paral.Exeter.get;
import static net.butfly.albacore.paral.steam.Steam.of;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Parals;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Maps;

final class Splitream<E, SELF extends Steam<E>> implements Steam<E>, Spliterator<E> {
	protected Exeter ex;
	protected boolean wait;
	protected final Spliterator<E> impl;

	public Splitream(Spliterator<E> impl) {
		super();
		this.impl = impl;
		ex = Exeter.of();
		wait = false;
	}

	@Override
	public Spliterator<E> spliterator() {
		return impl;
	}

	@Override
	public Steam<E> ex(Exeter ex) {
		this.ex = ex;
		return this;
	}

	@Override
	public Steam<E> setWait(boolean wait) {
		this.wait = wait;
		return this;
	}

	// =======================================
	@Override
	public List<E> collect() {
		List<E> l = Parals.list();
		get(each(ex, spliterator(), e -> {
			if (null != e) l.add(e);
		}, new LinkedBlockingQueue<>()));
		return l;
	}

	@Override
	public List<E> list() {
		List<E> l = Parals.list();
		eachs(e -> {
			if (null != e) l.add(e);
		});
		return l;
	}

	@Override
	public void eachs(Consumer<E> using) {
		eachs(spliterator(), using);
	}

	@Override
	public void each(Consumer<E> using) {
		BlockingQueue<Future<?>> fs = each(ex, spliterator(), using, new LinkedBlockingQueue<>());
		if (wait) Exeter.get(fs);
	}

	// =======================================
	@Override
	public Steam<E> filter(Predicate<E> checking) {
		return of(new Spliterator<E>() {
			@Override
			public int characteristics() {
				return impl.characteristics();
			}

			@Override
			public long estimateSize() {
				return impl.estimateSize();
			}

			@Override
			public boolean tryAdvance(Consumer<? super E> using) {
				AtomicBoolean d = new AtomicBoolean(false);
				while (!d.get())
					if (!impl.tryAdvance(e -> {
						boolean u = null != e;
						d.lazySet(u);
						if (u) using.accept(e);
					})) return false;
				return d.get();
			}

			@Override
			public Spliterator<E> trySplit() {
				Spliterator<E> ss = impl.trySplit();
				return null == ss ? null : of(ss).filter(checking).spliterator();
			}
		});
	}

	@Override
	public <R> Steam<R> map(Function<E, R> conv) {
		return of(map(impl, conv));
	}

	@Override
	public <R> Steam<R> map(Function<Steam<E>, Steam<R>> conv, int maxBatchSize) {
		return of(map(impl, conv, maxBatchSize));
	}

	@Override
	public <R> Steam<R> mapFlat(Function<E, Steam<R>> flat) {
		return of(mapFlat(impl, flat));
	}

	@Override
	public E reduce(BinaryOperator<E> accumulator) {
		return reduce(ex, impl, accumulator);
	}

	@Override
	public <R> Steam<Pair<E, R>> join(Function<E, R> joining) {
		return of(join(impl, joining));
	}

	@Override
	public Steam<E> union(Steam<E> another) {
		Spliterator<E> z1 = this, z2 = another.spliterator();
		return of(new Spliterator<E>() {
			@Override
			public int characteristics() {
				return chars.merge(z1.characteristics(), z2.characteristics());
			}

			@Override
			public long estimateSize() {
				long sz1 = z1.estimateSize();
				if (sz1 == Long.MAX_VALUE) return Long.MAX_VALUE;
				long sz2 = z2.estimateSize();
				if (sz1 == Long.MAX_VALUE || sz2 >= Long.MAX_VALUE - sz1) return Long.MAX_VALUE;
				return sz1 + sz2;
			}

			@Override
			public boolean tryAdvance(Consumer<? super E> using) {
				if (z1.tryAdvance(using)) return true;
				return z2.tryAdvance(using);
			}

			@Override
			public Spliterator<E> trySplit() {
				Spliterator<E> s = z1.trySplit();
				return null != s ? s : z2.trySplit();
			}
		});
	}

	@Override
	public <E1> Steam<Pair<E, E1>> join(Function<Steam<E>, Steam<E1>> joining, int maxBatchSize) {
		return null;
	}

	// ==================
	@Override
	public void batch(Consumer<Steam<E>> using, int maxBatchSize) {
		batch(ex, impl, using, maxBatchSize);
	}

	@Override
	public void partition(Consumer<Steam<E>> using, int minPartNum) {
		partition(ex, impl, using, minPartNum);
	}

	@Override
	public List<Steam<E>> partition(int minPartNum) {
		List<Steam<E>> l = Parals.list();
		get(partition(ex, impl, l::add, minPartNum));
		return l;
	}

	@Override
	public <K> void partition(BiConsumer<K, E> using, Function<E, K> keying) {
		partition(ex, impl, using, keying);
	}

	@Override
	public <K, V> Map<K, List<V>> partition(Function<E, K> keying, Function<E, V> valuing) {
		Map<K, List<V>> m = Maps.of();
		get(partition(ex, impl, (k, e) -> m.compute(k, (kk, l) -> {
			if (null != e) {
				if (null == l) l = Parals.list();
				l.add(valuing.apply(e));
			}
			return l;
		}), keying));
		return m;
	}

	@Override
	public <K, V> Map<K, V> partition(Function<E, K> keying, Function<E, V> valuing, BinaryOperator<V> reducing) {
		Map<K, V> m = Maps.of();
		get(partition(ex, impl, (k, e) -> m.compute(k, (kk, v) -> {
			if (null == e) return v;
			V vv = valuing.apply(e);
			if (null == v || null == vv) return vv;
			return reducing.apply(v, vv);
		}), keying));
		return m;
	}

	@Override
	public <K> void partition(BiConsumer<K, Steam<E>> using, Function<E, K> keying, int maxBatchSize) {
		partition(ex, impl, using, keying, maxBatchSize);
	}

	// ==================
	/** Using spliterator sequencially */
	static <E> void eachs(Spliterator<E> s, Consumer<E> using) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		while (s0.tryAdvance(using)) {}
	}

	/** Using spliterator parallelly with trySplit() */
	static <E> BlockingQueue<Future<?>> each(Exeter ex, Spliterator<E> s, Consumer<E> using, BlockingQueue<Future<?>> fs) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		Spliterator<E> ss;
		while (null != (ss = s0.trySplit())) {
			Spliterator<E> sss = ss;
			fs.offer(ex.submit((Runnable) () -> each(ex, sss, using, fs)));
		}
		fs.offer(ex.submit((Runnable) () -> eachs(s0, using)));
		return fs;
	}

	static <E, K> BlockingQueue<Future<?>> partition(Exeter ex, Spliterator<E> s, BiConsumer<K, E> using, Function<E, K> keying) {
		return each(ex, Objects.requireNonNull(s), e -> using.accept(keying.apply(e), e), new LinkedBlockingQueue<>());
	}

	static <E, K> BlockingQueue<Future<?>> partition(Exeter ex, Spliterator<E> s, BiConsumer<K, Steam<E>> using, Function<E, K> keying,
			int maxBatchSize) {
		Map<K, BlockingQueue<E>> map = Maps.of();
		BlockingQueue<Future<?>> fs = new LinkedBlockingQueue<>();
		return each(ex, Objects.requireNonNull(s), e -> map.compute(keying.apply(e), (k, l) -> {
			if (null == l) l = new LinkedBlockingQueue<>();
			l.offer(e);
			List<E> batch = Parals.list();
			l.drainTo(batch, maxBatchSize);
			if (l.isEmpty() || batch.size() > maxBatchSize) fs.offer(ex.submit(() -> using.accept(k, of(batch))));
			else l.addAll(batch);
			return l.isEmpty() ? null : l;
		}), fs);
	}

	static <E> E reduce(Exeter ex, Spliterator<E> s, BinaryOperator<E> accumulator) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		AtomicReference<E> r = new AtomicReference<>();
		get(each(ex, s0, e -> r.accumulateAndGet(e, (e1, e2) -> {
			if (null == e1) return e2;
			if (null == e2) return e1;
			return accumulator.apply(e1, e2);
		}), new LinkedBlockingQueue<>()));
		return r.get();
	}

	static <E, R> Spliterator<R> map(Spliterator<E> s, Function<E, R> conv) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		return new Spliterator<R>() {
			@Override
			public int characteristics() {
				return s0.characteristics();
			}

			@Override
			public long estimateSize() {
				return s0.estimateSize();
			}

			@Override
			public boolean tryAdvance(Consumer<? super R> using) {
				return s0.tryAdvance(e -> using.accept(conv.apply(e)));
			}

			@Override
			public Spliterator<R> trySplit() {
				Spliterator<E> ss = s0.trySplit();
				return null == ss ? null : map(ss, conv);
			}
		};
	}

	static <E, R> Spliterator<R> map(Spliterator<E> s, Function<Steam<E>, Steam<R>> conv, int maxBatchSize) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		boolean unbatch = maxBatchSize >= Integer.MAX_VALUE || maxBatchSize <= 0;
		return new Spliterator<R>() {
			private final BlockingQueue<E> cache = new LinkedBlockingQueue<>();

			@Override
			public int characteristics() {
				return s0.characteristics();
			}

			@Override
			public long estimateSize() {
				return s0.estimateSize();
			}

			@Override
			public boolean tryAdvance(Consumer<? super R> using) {
				return s0.tryAdvance(e -> {
					List<E> batch = Parals.list();
					cache.offer(e);
					cache.drainTo(batch, maxBatchSize);
					if (batch.size() >= maxBatchSize || cache.isEmpty()) conv.apply(Steam.of(batch)).each(using::accept);
					else for (E ee : batch)
						cache.offer(ee);
				});
			}

			@Override
			public Spliterator<R> trySplit() {
				Spliterator<E> ss = s0.trySplit();
				if (null == ss) return null;
				else if (unbatch) return conv.apply(of(ss)).spliterator();
				else return map(ss, conv, maxBatchSize);
			}
		};
	}

	static <E, R> Spliterator<R> mapFlat(Spliterator<E> impl, Function<E, Steam<R>> flat) {
		Spliterator<E> s0 = Objects.requireNonNull(impl);
		return new Spliterator<R>() {
			@Override
			public int characteristics() {
				return s0.characteristics() //
						& chars.NON_SIZED & chars.NON_SUBSIZED & chars.NON_DISTINCT & chars.NON_ORDERED & chars.NON_SORTED;
			}

			@Override
			public long estimateSize() {
				return Long.MAX_VALUE;
			}

			@Override
			public boolean tryAdvance(Consumer<? super R> using) {
				return s0.tryAdvance(e -> flat.apply(e).each(using::accept));
			}

			@Override
			public Spliterator<R> trySplit() {
				Spliterator<E> ss = s0.trySplit();
				return null == ss ? null : mapFlat(ss, flat);
			}
		};
	}

	static <E, R> Spliterator<Pair<E, R>> join(Spliterator<E> s, Function<E, R> joining) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		return new Spliterator<Pair<E, R>>() {
			@Override
			public int characteristics() {
				return s0.characteristics();
			}

			@Override
			public long estimateSize() {
				return s0.estimateSize();
			}

			@Override
			public boolean tryAdvance(Consumer<? super Pair<E, R>> using) {
				return s0.tryAdvance(e -> using.accept(new Pair<>(e, joining.apply(e))));
			}

			@Override
			public Spliterator<Pair<E, R>> trySplit() {
				Spliterator<E> ss = s0.trySplit();
				return null == ss ? null : join(ss, joining);
			}
		};
	}

	static <E> BlockingQueue<Future<?>> partition(Exeter ex, Spliterator<E> s, Consumer<Steam<E>> using, int minPartNum) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		BlockingQueue<Future<?>> fs = new LinkedBlockingQueue<>();
		for (int i = 0; i < minPartNum; i++) {
			Spliterator<E> ss = s0.trySplit();
			if (null != ss) fs.offer(ex.submit(() -> using.accept(of(ss))));
			else break;
		}
		fs.offer(ex.submit(() -> using.accept(of(s0))));
		return fs;
	}

	static <E> BlockingQueue<Future<?>> batch(Exeter ex, Spliterator<E> s, Consumer<Steam<E>> using, int maxBatchSize) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		BlockingQueue<Future<?>> fs = new LinkedBlockingQueue<>();
		while (s0.estimateSize() > maxBatchSize) {
			Spliterator<E> ss = s0.trySplit();
			if (null != ss) fs.offer(ex.submit(() -> using.accept(of(ss))));
			else break;
		}
		fs.offer(ex.submit(() -> using.accept(of(s0))));
		return fs;
	}

	// =========================
	@Override
	public int characteristics() {
		return impl.characteristics();
	}

	@Override
	public long estimateSize() {
		return impl.estimateSize();
	}

	@Override
	public boolean tryAdvance(Consumer<? super E> using) {
		return impl.tryAdvance(using);
	}

	@Override
	public Spliterator<E> trySplit() {
		return impl.trySplit();
	}

	// =========================
	interface chars {
		final int ALL = SORTED | DISTINCT | SUBSIZED | ORDERED | SIZED | NONNULL | CONCURRENT | IMMUTABLE;
		final int NON_ALL = ~ALL;
		// never merge
		final int NON_SORTED = ~SORTED;
		final int NON_DISTINCT = ~DISTINCT;
		// and merge
		final int NON_SUBSIZED = ~SUBSIZED;
		final int NON_ORDERED = ~ORDERED;
		final int NON_SIZED = ~SIZED;
		final int NON_NONNULL = ~NONNULL;
		final int NON_CONCURRENT = ~CONCURRENT;
		// or merge
		final int NON_IMMUTABLE = ~IMMUTABLE;

		static boolean has(int ch, int bit) {
			return (ch & bit) == ch;
		}

		static int merge(int ch1, int ch2) {
			int and = ch1 & ch2 & SUBSIZED & ORDERED & SIZED & NONNULL & CONCURRENT;
			int or = (ch1 | ch2) & IMMUTABLE;
			int non = NON_SORTED | NON_DISTINCT;
			return ch1 & ch2 & NON_ALL | and | or | non;
		}
	}
}
