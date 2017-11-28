package net.butfly.albacore.paral.steam;

import static net.butfly.albacore.paral.Exeter.get;
import static net.butfly.albacore.paral.steam.Sdream.of;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Parals;
import net.butfly.albacore.paral.split.BatchSpliterator;
import net.butfly.albacore.paral.split.ConcatSpliterator;
import net.butfly.albacore.paral.split.ConvedSpliterator;
import net.butfly.albacore.paral.split.FilteredSpliterator;
import net.butfly.albacore.paral.split.FlatedSpliterator;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Maps;

public final class Splidream<E, SELF extends Sdream<E>> implements Sdream<E>, Spliterator<E> {
	protected Exeter ex;
	protected boolean wait;
	protected final Spliterator<E> impl;

	public Splidream(Spliterator<E> impl) {
		super();
		this.impl = Objects.requireNonNull(impl);
		ex = Exeter.of();
		wait = false;
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

	@Override
	public Sdream<E> setWait(boolean wait) {
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
	public <R> Sdream<Pair<E, R>> join(Function<E, R> joining) {
		return map(e -> {
			if (null == e) return null;
			R r = joining.apply(e);
			if (null == r) return null;
			return new Pair<>(e, r);
		});
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

	/** Using spliterator parallelly with trySplit() */
	@Override
	public void each(Consumer<E> using) {
		BlockingQueue<Future<?>> fs = each(ex, spliterator(), using, new LinkedBlockingQueue<>());
		if (wait) Exeter.get(fs);
	}

	@Override
	public void batch(Consumer<Sdream<E>> using, int maxBatchSize) {
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
		List<Sdream<E>> l = Parals.list();
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
	public <K> void partition(BiConsumer<K, Sdream<E>> using, Function<E, K> keying, int maxBatchSize) {
		Map<K, BlockingQueue<E>> map = Maps.of();
		BlockingQueue<Future<?>> fs = new LinkedBlockingQueue<>();
		each(ex, impl, e -> map.compute(keying.apply(e), (k, l) -> {
			if (null == l) l = new LinkedBlockingQueue<>();
			l.offer(e);
			List<E> batch = Parals.list();
			l.drainTo(batch, maxBatchSize);
			if (l.isEmpty() || batch.size() > maxBatchSize) fs.offer(ex.submit(() -> using.accept(k, of(batch))));
			else l.addAll(batch);
			return l.isEmpty() ? null : l;
		}), fs);
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

	static <E, K> BlockingQueue<Future<?>> partition(Exeter ex, Spliterator<E> s, BiConsumer<K, Sdream<E>> using, Function<E, K> keying,
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

	static <E> BlockingQueue<Future<?>> partition(Exeter ex, Spliterator<E> s, Consumer<Sdream<E>> using, int minPartNum) {
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
}
