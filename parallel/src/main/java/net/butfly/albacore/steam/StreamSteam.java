package net.butfly.albacore.steam;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.utils.Pair;

/**
 * Not full lazy
 * 
 * @author butfly
 *
 * @param <E>
 */
public class StreamSteam<E> extends SteamBase<E, Stream<E>, StreamSteam<E>> implements Steam<E> {
	@Override
	public boolean next(Consumer<E> using) {
		throw new UnsupportedOperationException();
	}

	public StreamSteam(Stream<E> impl) {
		super(impl);
	}

	@Override
	public <R> Steam<R> map(Function<E, R> conv) {
		return Steam.wrap(impl.map(conv));
	}

	@Override
	public <R> Steam<R> mapFlat(Function<E, List<R>> flat) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public E reduce(BinaryOperator<E> accumulator) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <E1> Steam<Pair<E, E1>> join(Function<E, E1> func) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <K> Map<K, Steam<E>> groupBy(Function<E, K> keying) {
		Map<K, Steam<E>> m = new ConcurrentHashMap<>();
		for (Map.Entry<K, List<E>> e : impl.collect(Collectors.groupingBy(keying)).entrySet())
			m.put(e.getKey(), Steam.wrap(e.getValue().stream()));
		return m;
	}

	@Override
	public List<Steam<E>> partition(int parts) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<Steam<E>> batch(long maxBatch) {
		// TODO Auto-generated method stub
		return null;
	}
}
