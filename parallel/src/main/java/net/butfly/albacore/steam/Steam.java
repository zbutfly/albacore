package net.butfly.albacore.steam;

import java.util.List;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.utils.Pair;

public interface Steam<E> extends Spliterator<E> {
	Spliterator<E> spliterator();

	<R> Steam<R> map(Function<E, R> conv);

	<R> Steam<R> mapFlat(Function<E, List<R>> flat);

	E reduce(BinaryOperator<E> accumulator);

	<E1> Steam<Pair<E, E1>> join(Function<E, E1> joining);

	Steam<E> union(Steam<E> another);

	// ==================
	boolean next(Consumer<E> using);

	void partition(Consumer<E> using, int minPartNum);

	<K> void partition(BiConsumer<K, List<E>> using, Function<E, K> keying, int maxBatchSize);

	void batch(Consumer<List<E>> using, int maxBatchSize);

	void each(Consumer<E> using);

	// ==================
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
}
