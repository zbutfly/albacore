package net.butfly.albacore.paral;

import java.util.List;
import java.util.Map;
import java.util.Spliterator;

import net.butfly.albacore.io.lambda.BiConsumer;
import net.butfly.albacore.io.lambda.BinaryOperator;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.io.lambda.Predicate;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;

/**
 * empty sdream
 */
public class Emdream<E> implements Sdream<E> {
	private static final long serialVersionUID = -7699293250438844464L;

	@Override
	public List<E> list() {
		return Colls.list();
	}

	@Override
	public Spliterator<E> spliterator() {
		List<E> l = Colls.list();
		return l.spliterator();
	}

	@Override
	public Sdream<E> ex(Exeter ex) {
		return this;
	}

	@Override
	public Sdream<E> filter(Predicate<E> checking) {
		return this;
	}

	@Override
	public Sdream<Sdream<E>> batch(int maxBatchSize) {
		return Sdream.of();
	}

	@Override
	public <R> Sdream<R> map(Function<E, R> conv) {
		return Sdream.of();
	}

	@Override
	public <R> Sdream<R> map(Function<Sdream<E>, Sdream<R>> conv, int maxBatchSize) {
		return Sdream.of();
	}

	@Override
	public <R> Sdream<R> mapFlat(Function<E, Sdream<R>> flat) {
		return Sdream.of();
	}

	@Override
	public E reduce(BinaryOperator<E> accumulator) {
		return null;
	}

	@Override
	public <E1> Sdream<Pair<E, E1>> join(Function<Sdream<E>, Sdream<E1>> joining, int maxBatchSize) {
		return Sdream.of();
	}

	@Override
	public Sdream<E> union(Sdream<E> another) {
		return another;
	}

	@Override
	public void eachs(Consumer<E> using) {}

	@Override
	public void each(Consumer<E> using) {}

	@Override
	public void partition(Consumer<Sdream<E>> using, int minPartNum) {}

	@Override
	public <K> void partition(BiConsumer<K, E> using, Function<E, K> keying) {}

	@Override
	public <K> void partition(BiConsumer<K, Sdream<E>> using, Function<E, K> keying, int maxBatchSize) {}

	@Override
	public void batch(Consumer<Sdream<E>> using, int maxBatchSize) {}

	@Override
	public List<Sdream<E>> partition(int minPartNum) {
		return Colls.list();
	}

	@Override
	public <K, V> Map<K, List<V>> partition(Function<E, K> keying, Function<E, V> valuing) {
		return Maps.of();
	}

	@Override
	public <K, V> Map<K, V> partition(Function<E, K> keying, Function<E, V> valuing, BinaryOperator<V> reducing) {
		return Maps.of();
	}
}
