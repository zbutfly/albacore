package net.butfly.albacore.steam;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.utils.Pair;

public interface Steam<E> {
	boolean next(Consumer<E> using);

	<R> Steam<R> map(Function<E, R> conv);

	<R> Steam<R> mapFlat(Function<E, List<R>> flat);

	E reduce(BinaryOperator<E> accumulator);

	<E1> Steam<Pair<E, E1>> join(Function<E, E1> func);

	<K> Map<K, Steam<E>> groupBy(Function<E, K> keying);

	List<Steam<E>> partition(int parts);

	List<Steam<E>> batch(long maxBatch);

	static <E, S> Steam<E> wrap(Spliterator<E> impl) {
		return new SpliteratorSteam<>(impl);
	}

	static <E, S> Steam<E> wrap(Iterator<E> impl) {
		return new IteratorSteam<>(impl);
	}

	static <E, S> Steam<E> wrap(Stream<E> impl) {
		return new StreamSteam<>(impl);
	}
}
