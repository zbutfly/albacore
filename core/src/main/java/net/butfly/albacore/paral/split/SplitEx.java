package net.butfly.albacore.paral.split;

import static java.util.Spliterator.CONCURRENT;
import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.SIZED;
import static java.util.Spliterator.SORTED;
import static java.util.Spliterator.SUBSIZED;
import static net.butfly.albacore.paral.Exeters.DEFEX;
import static net.butfly.albacore.paral.steam.Steam.of;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;

import net.butfly.albacore.paral.steam.Steam;
import net.butfly.albacore.utils.Pair;

public interface SplitEx {
	static <E> E reduce(Spliterator<E> s, BinaryOperator<E> accumulator) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		AtomicReference<E> r = new AtomicReference<>();
		SplitEx.each(s0, e -> r.accumulateAndGet(e, (e1, e2) -> {
			if (null == e1) return e2;
			if (null == e2) return e1;
			return accumulator.apply(e1, e2);
		}));
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
					List<E> batch = list();
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
				return null == ss ? null : map(ss, conv, maxBatchSize);
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

	static <E> Spliterator<E> concat(Spliterator<E> s1, Spliterator<E> s2) {
		Spliterator<E> z1 = Objects.requireNonNull(s1), z2 = Objects.requireNonNull(s2);
		return new Spliterator<E>() {
			@Override
			public int characteristics() {
				return SplitEx.chars.merge(z1.characteristics(), z2.characteristics());
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
		};
	}

	static <E> List<E> list() {
		return new CopyOnWriteArrayList<>();
	}

	static <E> List<E> list(Spliterator<E> s) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		List<E> l = list();
		each(s0, l::add);
		return l;
	}

	/** Using spliterator sequencially */
	static <E> void eachs(Spliterator<E> s, Consumer<E> using) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		while (s0.tryAdvance(using)) {}
	}

	/** Using spliterator parallelly with trySplit() */
	static <E> void each(Spliterator<E> s, Consumer<E> using) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		if (!s0.hasCharacteristics(CONCURRENT)) eachs(s, using);
		boolean splited;
		do {
			Spliterator<E> ss = s0.trySplit();
			splited = null != ss;
			if (null != ss) DEFEX.submit((Runnable) () -> each(ss, using));
		} while (splited);
		DEFEX.submit((Runnable) () -> eachs(s0, using));
	}

	static <K, E> void partition(Spliterator<E> s, BiConsumer<K, E> using, Function<E, K> keying) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		each(s0, e -> using.accept(keying.apply(e), e));
	}

	static <K, E> void partition(Spliterator<E> s, BiConsumer<K, Steam<E>> using, Function<E, K> keying, int maxBatchSize) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		Map<K, BlockingQueue<E>> map = new ConcurrentHashMap<>();
		each(s0, e -> map.compute(keying.apply(e), (k, l) -> {
			if (null == l) l = new LinkedBlockingQueue<>();
			l.offer(e);
			List<E> batch = new CopyOnWriteArrayList<>();
			l.drainTo(batch, maxBatchSize);
			if (l.isEmpty() || batch.size() >= maxBatchSize) DEFEX.submit(() -> using.accept(k, of(batch)));
			else l.addAll(batch);
			return l.isEmpty() ? null : l;
		}));
	}

	static <E> void partition(Spliterator<E> s, Consumer<Steam<E>> using, int minPartNum) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		for (int i = 0; i < minPartNum; i++) {
			Spliterator<E> ss = s0.trySplit();
			if (null != ss) DEFEX.submit(() -> using.accept(of(ss)));
			else break;
		}
		DEFEX.submit(() -> using.accept(of(s0)));
	}

	static <E> void batch(Spliterator<E> s, Consumer<Steam<E>> using, int maxBatchSize) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		if (s0.hasCharacteristics(SUBSIZED)) while (s0.estimateSize() > maxBatchSize) {
			Spliterator<E> ss = s0.trySplit();
			if (null != ss) DEFEX.submit(() -> using.accept(of(ss)));
			else break;
		}
		DEFEX.submit(() -> using.accept(of(s0)));
	}

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
