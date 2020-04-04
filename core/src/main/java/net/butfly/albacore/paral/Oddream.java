package net.butfly.albacore.paral;

import static net.butfly.albacore.paral.Sdream.of;
import static net.butfly.albacore.paral.Sdream.of1;

import java.util.List;
import java.util.Map;
import java.util.Spliterator;

import net.butfly.albacore.io.lambda.BiConsumer;
import net.butfly.albacore.io.lambda.BinaryOperator;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.io.lambda.Predicate;
import net.butfly.albacore.paral.split.ConcatSpliterator;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;

public class Oddream<E> implements Sdream<E> {
	private static final long serialVersionUID = -4918238882462226640L;
	protected final E undly;

	public Oddream(E e) {
		undly = e;
	}

	@Override
	public List<E> list() {
		return Colls.list(undly);
	}

	@Override
	public Spliterator<E> spliterator() {
		return Colls.list(undly).spliterator();
	}

	@Override
	public Sdream<E> ex(Exeter ex) {
		return this;
	}

	// =======================================

	@Override
	public E reduce(BinaryOperator<E> accumulator) {
		return undly;
	}

	// conving =======================================
	@Override
	public Sdream<E> filter(Predicate<E> checking) {
		return checking.test(undly) ? this : Sdream.of();
	}

	@Override
	@Deprecated
	public Sdream<Sdream<E>> batch(int maxBatchSize) {
		return of(of(undly));
	}

	@Override
	public <R> Sdream<R> map(Function<E, R> conv, Function<? super R, ?> dstschema) {
		return of(conv.apply(undly));
	}

	@Override
	@Deprecated
	public <R> Sdream<R> map(Function<Sdream<E>, Sdream<R>> conv, int maxBatchSize, Function<? super R, ?> dstschema) {
		return conv.apply(of1(undly));
	}

	@Override
	public <R> Sdream<R> mapFlat(Function<E, Sdream<R>> flat, Function<? super R, ?> dstschema) {
		return flat.apply(undly);
	}

	@Override
	@Deprecated
	public Sdream<E> union(Sdream<E> another) {
		return of(new ConcatSpliterator<>(spliterator(), another.spliterator()));
	}

	@Override
	public <E1> Sdream<Pair<E, E1>> join(Function<Sdream<E>, Sdream<E1>> joining, int maxBatchSize) {
		return null;
	}

	// using ==================
	/** Using spliterator sequencially */
	@Override
	public void eachs(Consumer<E> using) {
		using.accept(undly);
	}

	/**
	 * Using spliterator parallelly with trySplit()
	 * 
	 * @return
	 */
	@Override
	public void each(Consumer<E> using) {
		using.accept(undly);
	}

	@Override
	public void batch(Consumer<Sdream<E>> using, int maxBatchSize) {
		using.accept(of1(undly));
	}

	@Override
	public void partition(Consumer<Sdream<E>> using, int minPartNum) {
		using.accept(of1(undly));
	}

	@Override
	public <K> void partition(BiConsumer<K, Sdream<E>> using, Function<E, K> keying, int maxBatchSize) {
		using.accept(keying.apply(undly), of1(undly));
	}

	@Override
	public List<Sdream<E>> partition(int minPartNum) {
		return Colls.list(this);
	}

	@Override
	public <K> void partition(BiConsumer<K, E> using, Function<E, K> keying) {
		using.accept(keying.apply(undly), undly);
	}

	@Override
	public <K, V> Map<K, List<V>> partition(Function<E, K> keying, Function<E, V> valuing) {
		return Maps.of(keying.apply(undly), Colls.list(valuing.apply(undly)));
	}

	@Override
	public <K, V> Map<K, V> partition(Function<E, K> keying, Function<E, V> valuing, BinaryOperator<V> reducing) {
		return Maps.of(keying.apply(undly), valuing.apply(undly));
	}
}
