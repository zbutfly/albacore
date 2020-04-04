package net.butfly.albacore.paral;

import static net.butfly.albacore.utils.logger.LogExec.tryExec;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;

import net.butfly.albacore.io.lambda.BiConsumer;
import net.butfly.albacore.io.lambda.BinaryOperator;
import net.butfly.albacore.io.lambda.Consumer;
import net.butfly.albacore.io.lambda.Function;
import net.butfly.albacore.io.lambda.IntFunction;
import net.butfly.albacore.io.lambda.Predicate;
import net.butfly.albacore.paral.split.Splidream;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.parallel.Lambdas;

public interface Sdream<E> extends Serializable {
	static final Logger logger = Logger.getLogger(Sdream.class);

	default Sdream<E> ex(ExecutorService ex) {
		return ex(Exeter.of(ex));
	}

	Spliterator<E> spliterator();

	Sdream<E> ex(Exeter ex);

	default Sdream<E> ex() {
		return ex(Exeter.of());
	}

	default Sdream<E> nonNull() {
		return filter(e -> null != e);
	}

	Sdream<E> filter(Predicate<E> checking);

	Sdream<Sdream<E>> batch(int maxBatchSize);

	/**
	 * for debugging / logging ONLY, since it is not stable.
	 */
	default Sdream<E> peek(Consumer<E> conv) {
		return map(e -> {
			tryExec(() -> conv.accept(e));
			return e;
		});
	}

	default <R> Sdream<R> map(Function<E, R> conv) {
		return map(conv, r -> null);
	}

	<R> Sdream<R> map(Function<E, R> conv, Function<? super R, ?> dstschema);

	@Deprecated
	default <R> Sdream<R> map(Function<Sdream<E>, Sdream<R>> conv, int maxBatchSize) {
		return map(conv, maxBatchSize, r -> null);
	}

	@Deprecated
	default <R> Sdream<R> map(Function<Sdream<E>, Sdream<R>> conv, int maxBatchSize, Function<? super R, ?> dstschema) {
		return conv.apply(this);
	}

	default <R> Sdream<R> mapFlat(Function<E, Sdream<R>> flat) {
		return mapFlat(flat, r -> null);
	}

	<R> Sdream<R> mapFlat(Function<E, Sdream<R>> flat, Function<? super R, ?> dstschema);

	E reduce(BinaryOperator<E> accumulator);

	default <E1> Sdream<Pair<E, E1>> join(Function<E, E1> joining) {
		return map(e -> {
			if (null == e) return null;
			E1 r = joining.apply(e);
			if (null == r) return null;
			return new Pair<>(e, r);
		});
	}

	<E1> Sdream<Pair<E, E1>> join(Function<Sdream<E>, Sdream<E1>> joining, int maxBatchSize);

	Sdream<E> union(Sdream<E> another);

	// ==================
	default List<E> collect() {
		return list();
	}

	default E[] array(IntFunction<E[]> arr) {
		List<E> l = list();
		return l.toArray(arr.apply(l.size()));
	}

	default List<E> list() {
		List<E> l = Colls.list();
		eachs(e -> {
			if (null != e) l.add(e);
		});
		return l;
	}

	default BlockingQueue<E> queue() {
		return new LinkedBlockingQueue<>(list());
	}

	default Set<E> distinct() {
		Set<E> s = Colls.distinct();
		eachs(s::add);
		return s;
	}

	void eachs(Consumer<E> using);

	void each(Consumer<E> using);

	default String joinAsString(Function<E, CharSequence> conv, CharSequence... separators) {
		return String.join(null == separators || separators.length == 0 ? "" : String.join("", separators), map(conv).list());
	}

	default String joinAsString(CharSequence prefix, Function<E, CharSequence> conv, CharSequence... separators) {
		String s = joinAsString(conv, separators);
		return null == prefix || prefix.length() == 0 ? s : prefix + s;
	}

	default long count() {
		return map(e -> 1L).reduce(Lambdas.sumLong());
	}

	default void partition(Consumer<Sdream<E>> using, int minPartNum) {
		using.accept(this);
	}

	<K> void partition(BiConsumer<K, E> using, Function<E, K> keying);

	<K> void partition(BiConsumer<K, Sdream<E>> using, Function<E, K> keying, int maxBatchSize);

	default void batch(Consumer<Sdream<E>> using, int maxBatchSize) {
		using.accept(this);
	}

	static <E, S> Sdream<E> of(Spliterator<E> impl) {
		return new Splidream<>(impl);
	}

	static <K, V> Sdream<Map.Entry<K, V>> of(Map<K, V> map) {
		return of(map.entrySet());
	}

	static <E, S> Sdream<E> of(Iterator<E> itor) {
		if (!itor.hasNext()) return of();
		return of(Spliterators.spliteratorUnknownSize(itor, Spliterator.ORDERED));
	}

	static <E, S> Sdream<E> of(Iterable<E> impl) {
		if (Colls.empty(impl)) return of();
		if (impl instanceof Collection) return new Lisdream<>(impl);
		return new Splidream<>(impl.spliterator());
	}

	/** @deprecated Terminal of the stream */
	@Deprecated
	static <E, S> Sdream<E> of(Stream<E> impl) {
		return new Splidream<>(impl.spliterator());
	}

	static <V> Sdream<V> of1(V t) {
		return null == t ? of() : new Oddream<>(t);
	}

	@SafeVarargs
	static <V> Sdream<V> of(V... t) {
		if (null == t || t.length == 0) return of();
		if (t.length == 1) return of1(t[0]);
		return new Lisdream<>(Colls.list(t));
	}

	static <V> Sdream<V> of() {
		return new Emdream<>();
	}

	List<Sdream<E>> partition(int minPartNum);

	<K, V> Map<K, List<V>> partition(Function<E, K> keying, Function<E, V> valuing);

	<K, V> Map<K, V> partition(Function<E, K> keying, Function<E, V> valuing, BinaryOperator<V> reducing);

	/** Simple partition to Map<>, just ignore duplicated keys */
	default <K, V> Map<K, V> partitions(Function<E, K> keying, Function<E, V> valuing) {
		return partition(keying, valuing, Lambdas.nullOr());
	}
}
