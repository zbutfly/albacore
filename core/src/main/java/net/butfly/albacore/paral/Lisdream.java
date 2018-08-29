package net.butfly.albacore.paral;

import static net.butfly.albacore.paral.Sdream.of;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.butfly.albacore.io.lambda.BiConsumer;
import net.butfly.albacore.io.lambda.BinaryOperator;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.io.lambda.Predicate;
import net.butfly.albacore.paral.split.BatchSpliterator;
import net.butfly.albacore.paral.split.ConcatSpliterator;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;

public final class Lisdream<E> implements Sdream<E>/* , List<E> */ {
	private static final long serialVersionUID = -4918238882462226640L;
	protected final BlockingQueue<E> undly;

	public Lisdream(Iterable<E> impl) {
		undly = constr(impl);
	}

	@Override
	public List<E> list() {
		return Colls.list(undly);
	}

	@Override
	public Spliterator<E> spliterator() {
		return undly.spliterator();
	}

	@Override
	public Sdream<E> ex(Exeter ex) {
		return this;
	}

	// =======================================

	@Override
	public E reduce(BinaryOperator<E> accumulator) {
		if (undly.isEmpty()) return null;
		E first = undly.poll();
		E next;
		while (null != (next = undly.poll()))
			first = accumulator.apply(first, next);
		return first;
	}

	// conving =======================================
	@Override
	public Sdream<E> filter(Predicate<E> checking) {
		List<E> l = Colls.list();
		undly.forEach(e -> {
			if (checking.test(e)) l.add(e);
		});
		return new Lisdream<>(l);
	}

	@Override
	public Sdream<Sdream<E>> batch(int maxBatchSize) {
		return of(new BatchSpliterator<>(spliterator(), maxBatchSize));
	}

	@Override
	public <R> Sdream<R> map(Function<E, R> conv) {
		List<R> l = Colls.list();
		undly.forEach(e -> {
			R r = conv.apply(e);
			if (null != r) l.add(r);
		});
		return new Lisdream<>(l);
	}

	@Override
	@Deprecated
	public <R> Sdream<R> map(Function<Sdream<E>, Sdream<R>> conv, int maxBatchSize) {
		return conv.apply(this);
	}

	@Override
	public <R> Sdream<R> mapFlat(Function<E, Sdream<R>> flat) {
		List<R> l = Colls.list();
		undly.forEach(e -> {
			flat.apply(e).eachs(r -> {
				if (null != r) l.add(r);
			});
		});
		return new Lisdream<>(l);
	}

	@Override
	public Sdream<E> union(Sdream<E> another) {
		if (another instanceof Lisdream) {
			List<E> l = Colls.list();
			l.addAll(undly);
			l.addAll(((Lisdream<E>) another).undly);
			return new Lisdream<>(l);
		} else return of(new ConcatSpliterator<>(spliterator(), another.spliterator()));
	}

	@Override
	public <E1> Sdream<Pair<E, E1>> join(Function<Sdream<E>, Sdream<E1>> joining, int maxBatchSize) {
		return null;
	}

	// using ==================
	/** Using spliterator sequencially */
	@Override
	public void eachs(Consumer<E> using) {
		undly.forEach(using);
	}

	/**
	 * Using spliterator parallelly with trySplit()
	 * 
	 * @return
	 */
	@Override
	public void each(Consumer<E> using) {
		undly.forEach(using);
	}

	@Override
	public <K> void partition(BiConsumer<K, E> using, Function<E, K> keying) {
		undly.forEach(e -> using.accept(keying.apply(e), e));
	}

	@Override
	public <K> void partition(BiConsumer<K, Sdream<E>> using, Function<E, K> keying, int maxBatchSize) {
		Maps.of(undly, keying).forEach((k, l) -> using.accept(k, new Lisdream<>(l)));
	}

	@Override
	public List<Sdream<E>> partition(int minPartNum) {
		int s = undly.size() / minPartNum;
		List<Sdream<E>> ll = Colls.list();
		List<E> l = Colls.list();
		for (E e : undly) {
			l.add(e);
			if (l.size() > s) {
				ll.add(new Lisdream<>(l));
				l = Colls.list();
			}
		}
		if (!l.isEmpty()) ll.add(new Lisdream<>(l));
		return ll;
	}

	@Override
	public <K, V> Map<K, List<V>> partition(Function<E, K> keying, Function<E, V> valuing) {
		return Maps.of(undly, keying, valuing);
	}

	@Override
	public <K, V> Map<K, V> partition(Function<E, K> keying, Function<E, V> valuing, BinaryOperator<V> reducing) {
		Map<K, List<V>> m = Maps.of();
		undly.forEach(e -> m.computeIfAbsent(keying.apply(e), k -> Colls.list()).add(valuing.apply(e)));
		Map<K, V> mm = Maps.of();
		m.forEach((k, l) -> mm.put(k, new Lisdream<>(l).reduce(reducing)));
		return mm;
	}

	private static <E> BlockingQueue<E> constr(Iterable<E> impl) {
		if (impl instanceof BlockingQueue) return (BlockingQueue<E>) impl;
		if (impl instanceof Collection) return new LinkedBlockingQueue<E>((Collection<E>) impl);
		return new LinkedBlockingQueue<E>(Colls.list(impl));
	}
}
