package net.butfly.albacore.paral.steam;

import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.google.common.base.Joiner;

import net.butfly.albacore.paral.Exeter;
import net.butfly.albacore.paral.Suppliterator;
import net.butfly.albacore.utils.Pair;

public interface Steam<E> {
	Spliterator<E> spliterator();

	Steam<E> setWait(boolean wait);

	default Steam<E> ex(ExecutorService ex) {
		return ex(Exeter.of(ex));
	}

	Steam<E> ex(Exeter ex);

	default Steam<E> ex() {
		return ex(Exeter.of());
	}

	default Steam<E> unex() {
		return ex(Exeter.synchro);
	}

	/** Terminal!! */
	default Steam<E> nonNull() {
		return filter(e -> null != e);
	}

	/** Terminal!! */
	Steam<E> filter(Predicate<E> checking);

	<R> Steam<R> map(Function<E, R> conv);

	<R> Steam<R> map(Function<Steam<E>, Steam<R>> conv, int maxBatchSize);

	<R> Steam<R> mapFlat(Function<E, Steam<R>> flat);

	E reduce(BinaryOperator<E> accumulator);

	<E1> Steam<Pair<E, E1>> join(Function<E, E1> joining);

	<E1> Steam<Pair<E, E1>> join(Function<Steam<E>, Steam<E1>> joining, int maxBatchSize);

	Steam<E> union(Steam<E> another);

	// ==================
	List<E> collect();

	List<E> list();

	void eachs(Consumer<E> using);

	void each(Consumer<E> using);

	default String joinAsString(Function<E, CharSequence> conv, CharSequence... separators) {
		return Joiner.on(null == separators || separators.length == 0 ? "" : Joiner.on("").join(separators)).join(map(conv).list());
	}

	default String joinAsString(CharSequence prefix, Function<E, CharSequence> conv, CharSequence... separators) {
		String s = joinAsString(conv, separators);
		return null == prefix || prefix.length() == 0 ? s : prefix + s;
	}

	void partition(Consumer<Steam<E>> using, int minPartNum);

	<K> void partition(BiConsumer<K, E> using, Function<E, K> keying);

	<K> void partition(BiConsumer<K, Steam<E>> using, Function<E, K> keying, int maxBatchSize);

	void batch(Consumer<Steam<E>> using, int maxBatchSize);

	static <E, S> Steam<E> of(Spliterator<E> impl) {
		return new Splitream<>(impl);
	}

	static <E, S> Steam<E> of(Iterable<E> impl) {
		return new Splitream<>(impl.spliterator());
	}

	/** @deprecated Terminal of the stream */
	@Deprecated
	static <E, S> Steam<E> of(Stream<E> impl) {
		return new Splitream<>(impl.spliterator());
	}

	@SafeVarargs
	static <V> Steam<V> of(V... t) {
		return of(new CopyOnWriteArrayList<>(t).spliterator());
	}

	public static <V> Steam<V> of(Supplier<V> get, long size, Supplier<Boolean> ending) {
		return of(new Suppliterator<>(get, size, ending));
	}

	// ==================
	List<Steam<E>> partition(int minPartNum);

	<K, V> Map<K, List<V>> partition(Function<E, K> keying, Function<E, V> valuing);

	<K, V> Map<K, V> partition(Function<E, K> keying, Function<E, V> valuing, BinaryOperator<V> reducing);
}
