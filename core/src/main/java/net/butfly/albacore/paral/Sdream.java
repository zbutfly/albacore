package net.butfly.albacore.paral;

import static net.butfly.albacore.utils.logger.LogExec.tryExec;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.base.Joiner;

import net.butfly.albacore.paral.split.Splidream;
import net.butfly.albacore.utils.Pair;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.parallel.Lambdas;

public interface Sdream<E> {
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

	<R> Sdream<R> map(Function<E, R> conv);

	@Deprecated
	<R> Sdream<R> map(Function<Sdream<E>, Sdream<R>> conv, int maxBatchSize);

	<R> Sdream<R> mapFlat(Function<E, Sdream<R>> flat);

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
	Optional<E> next();

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
		BlockingQueue<E> l = new LinkedBlockingQueue<>();
		eachs(e -> {
			if (null != e) l.add(e);
		});
		return l;
	}

	default Set<E> distinct() {
		Set<E> s = Colls.distinct();
		eachs(s::add);
		return s;
	}

	void eachs(Consumer<E> using);

	void each(Consumer<E> using);

	default String joinAsString(Function<E, CharSequence> conv, CharSequence... separators) {
		return Joiner.on(null == separators || separators.length == 0 ? "" : Joiner.on("").join(separators)).join(map(conv).list());
	}

	default String joinAsString(CharSequence prefix, Function<E, CharSequence> conv, CharSequence... separators) {
		String s = joinAsString(conv, separators);
		return null == prefix || prefix.length() == 0 ? s : prefix + s;
	}

	default long count() {
		return map(e -> 1L).reduce(Lambdas.sumLong());
	}

	void partition(Consumer<Sdream<E>> using, int minPartNum);

	<K> void partition(BiConsumer<K, E> using, Function<E, K> keying);

	<K> void partition(BiConsumer<K, Sdream<E>> using, Function<E, K> keying, int maxBatchSize);

	void batch(Consumer<Sdream<E>> using, int maxBatchSize);

	static <E, S> Sdream<E> of(Spliterator<E> impl) {
		return new Splidream<>(impl);
	}

	static <K, V> Sdream<Map.Entry<K, V>> of(Map<K, V> map) {
		return new Splidream<>(map.entrySet().spliterator());
	}

	static <E, S> Sdream<E> of(Iterable<E> impl) {
		return new Splidream<>(impl.spliterator());
	}

	/** @deprecated Terminal of the stream */
	@Deprecated
	static <E, S> Sdream<E> of(Stream<E> impl) {
		return new Splidream<>(impl.spliterator());
	}

	static <V> Sdream<V> of1(V t) {
		CopyOnWriteArrayList<V> l = new CopyOnWriteArrayList<>();
		l.add(t);
		return of(l.spliterator());
	}

	@SafeVarargs
	static <V> Sdream<V> of(V... t) {
		return of(Colls.list(t).spliterator());
	}

	static <V> Sdream<V> of() {
		return of(Colls.<V> list().spliterator());
	}

	List<Sdream<E>> partition(int minPartNum);

	<K, V> Map<K, List<V>> partition(Function<E, K> keying, Function<E, V> valuing);

	<K, V> Map<K, V> partition(Function<E, K> keying, Function<E, V> valuing, BinaryOperator<V> reducing);

	/** Simple partition to Map<>, just ignore duplicated keys */
	default <K, V> Map<K, V> partitions(Function<E, K> keying, Function<E, V> valuing) {
		return partition(keying, valuing, Lambdas.nullOr());
	}
}
