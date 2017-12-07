package net.butfly.albacore.paral.split;

import static net.butfly.albacore.paral.Exeter.getn;
import static net.butfly.albacore.paral.Sdream.of;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;

public final class Splidream<E, SELF extends Sdream<E>> extends WrapperSpliterator<E> implements Sdream<E>, Spliterator<E> {
	protected Exeter ex;

	public Splidream(Spliterator<E> impl) {
		super(impl);
		ex = Exeter.of();
	}

	@Override
	public Spliterator<E> spliterator() {
		return impl;
	}

	@Override
	public Sdream<E> ex(Exeter ex) {
		this.ex = ex;
		return this;
	}

	// =======================================

	@Override
	public List<E> collect() {
		List<E> l = Colls.list();
		eachs(e -> {
			if (null != e) l.add(e);
		});
		return l;
	}

	@Override
	public E reduce(BinaryOperator<E> accumulator) {
		return reduce(ex, impl, accumulator);
	}

	// conving =======================================
	@Override
	public Sdream<E> filter(Predicate<E> checking) {
		return of(new FilteredSpliterator<>(impl, checking));
	}

	@Override
	public Sdream<Sdream<E>> batch(int maxBatchSize) {
		return of(new BatchSpliterator<>(impl, maxBatchSize));
	}

	@Override
	public <R> Sdream<R> map(Function<E, R> conv) {
		return of(new ConvedSpliterator<>(impl, conv));
	}

	@Override
	public <R> Sdream<R> map(Function<Sdream<E>, Sdream<R>> conv, int maxBatchSize) {
		return of(new BatchSpliterator<E>(impl, maxBatchSize)).map(conv).mapFlat(r -> r);
	}

	@Override
	public <R> Sdream<R> mapFlat(Function<E, Sdream<R>> flat) {
		return of(new FlatedSpliterator<>(map(flat).spliterator()));
	}

	@Override
	public Sdream<E> union(Sdream<E> another) {
		return of(new ConcatSpliterator<>(impl, another.spliterator()));
	}

	@Override
	public <E1> Sdream<Pair<E, E1>> join(Function<Sdream<E>, Sdream<E1>> joining, int maxBatchSize) {
		return null;
	}

	// using ==================
	/** Using spliterator sequencially */
	@Override
	public void eachs(Consumer<E> using) {
		eachs(spliterator(), using);
	}

	/**
	 * Using spliterator parallelly with trySplit()
	 * 
	 * @return
	 */
	@Override
	public void each(Consumer<E> using) {
		each(ex, spliterator(), using);
	}

	@Override
	public void batch(Consumer<Sdream<E>> using, int maxBatchSize) {
		batch(maxBatchSize).each(using);
		BlockingQueue<Future<?>> fs = new LinkedBlockingQueue<>();
		while (impl.estimateSize() > maxBatchSize) {
			Spliterator<E> ss = impl.trySplit();
			if (null != ss) fs.offer(ex.submit(() -> using.accept(of(ss))));
			else break;
		}
		fs.offer(ex.submit(() -> using.accept(of(impl))));
	}

	@Override
	public void partition(Consumer<Sdream<E>> using, int minPartNum) {
		partition(ex, impl, using, minPartNum);
	}

	@Override
	public List<Sdream<E>> partition(int minPartNum) {
		List<Sdream<E>> l = Colls.list();
		getn(partition(ex, impl, l::add, minPartNum));
		return l;
	}

	@Override
	public <K> void partition(BiConsumer<K, E> using, Function<E, K> keying) {
		partition(ex, impl, using, keying);
	}

	@Override
	public <K, V> Map<K, List<V>> partition(Function<E, K> keying, Function<E, V> valuing) {
		Map<K, List<V>> m = Maps.of();
		List<Future<?>> fs = Colls.list();
		for (Spliterator<E> s : split(spliterator()))
			fs.add(ex.submit((Runnable) () -> {
				eachs(s, e -> {
					if (null == e) return;
					K key = keying.apply(e);
					if (null == key) return;
					m.compute(key, (k, l) -> {
						if (null == l) l = Colls.list();
						V v = valuing.apply(e);
						if (null != v) l.add(v);
						return l;
					});
				});
			}));
		getn(fs);
		return m;
	}

	@Override
	public <K, V> Map<K, V> partition(Function<E, K> keying, Function<E, V> valuing, BinaryOperator<V> reducing) {
		Map<K, V> m = Maps.of();
		List<Future<?>> fs = Colls.list();
		for (Spliterator<E> s : split(spliterator()))
			fs.add(ex.submit((Runnable) () -> eachs(s, e -> {
				if (null == e) return;
				K key = keying.apply(e);
				if (null == key) return;
				m.compute(key, (k, v) -> {
					V vv = valuing.apply(e);
					if (null == vv) return v;
					if (null == v) return vv;
					return reducing.apply(v, vv);
				});
			})));
		getn(fs);
		return m;
	}

	@Override
	public <K> void partition(BiConsumer<K, Sdream<E>> using, Function<E, K> keying, int maxBatchSize) {
		partition(ex, impl, using, keying, maxBatchSize);
	}

	// chars =========================
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

	// internal =============================
	/** Using spliterator sequencially */
	static <E> void eachs(Spliterator<E> s, Consumer<E> using) {
		Spliterator<E> s0 = Objects.requireNonNull(s);
		while (s0.tryAdvance(using)) {}
	}

	/** Using spliterator parallelly with trySplit() */
	static <E> void each(Exeter ex, Spliterator<E> s, Consumer<E> using) {
		ex.submit(() -> {
			for (Spliterator<E> ss : split(Objects.requireNonNull(s)))
				ex.submit((Runnable) () -> eachs(ss, using));
		});
	}

	static <E, K> void partition(Exeter ex, Spliterator<E> s, BiConsumer<K, E> using, Function<E, K> keying) {
		each(ex, Objects.requireNonNull(s), e -> {
			using.accept(keying.apply(e), e);
		});
	}

	static <E, K> void partition(Exeter ex, Spliterator<E> s, BiConsumer<K, Sdream<E>> using, Function<E, K> keying, int maxBatchSize) {
		Map<K, BlockingQueue<E>> map = Maps.of();
		each(ex, Objects.requireNonNull(s), e -> map.compute(keying.apply(e), (k, l) -> {
			if (null == l) l = new LinkedBlockingQueue<>();
			l.offer(e);
			return checkBatch(ex, l, batch -> using.accept(k, of(batch)), maxBatchSize);
		}));
	}

	static <E> BlockingQueue<E> checkBatch(Exeter ex, BlockingQueue<E> l, Consumer<Collection<E>> using, int maxBatchSize) {
		List<E> batch = Colls.list();
		l.drainTo(batch, maxBatchSize);
		if (l.isEmpty() || batch.size() > maxBatchSize) ex.submit(() -> using.accept(batch));
		else l.addAll(batch);
		return l.isEmpty() ? null : l;
	}

	static <E> E reduce(Exeter ex, Spliterator<E> s, BinaryOperator<E> accumulator) {
		AtomicReference<E> r = new AtomicReference<>();
		List<Future<?>> fs = Colls.list();
		for (Spliterator<E> ss : split(Objects.requireNonNull(s)))
			fs.add(ex.submit((Runnable) () -> eachs(ss, e -> r.accumulateAndGet(e, accumulator::apply))));
		getn(fs);
		return r.get();
	}

	static <E> List<Spliterator<E>> split(Spliterator<E> origin) {
		List<Spliterator<E>> l = Colls.list(origin);
		l.add(origin);
		Spliterator<E> s;
		while (null != (s = origin.trySplit()))
			l.add(s);
		return l;
	}

	static <E> List<Spliterator<E>> split(Spliterator<E> origin, int partNum) {
		List<Spliterator<E>> l = Colls.list(origin);
		if (partNum > 1) {
			AtomicInteger parts = new AtomicInteger();
			l.add(origin);
			Spliterator<E> s;
			while (parts.incrementAndGet() < partNum) {
				if (null == (s = origin.trySplit())) return l;
				l.add(s);
			}
		}
		return l;
	}

	static <E> BlockingQueue<Future<?>> partition(Exeter ex, Spliterator<E> s, Consumer<Sdream<E>> using, int minPartNum) {
		BlockingQueue<Future<?>> fs = new LinkedBlockingQueue<>();
		for (Spliterator<E> ss : split(Objects.requireNonNull(s), minPartNum))
			fs.offer(ex.submit(() -> using.accept(of(ss))));
		return fs;
	}
}
