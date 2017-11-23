package net.butfly.albacore.steam;

import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;

import net.butfly.albacore.utils.Pair;

public class SpliteratorSteam<E> extends SteamBase<E, Spliterator<E>, SpliteratorSteam<E>> implements Steam<E> {
	@Override
	public boolean next(Consumer<E> using) {
		return impl.tryAdvance((Consumer<? super E>) using);
	}

	public SpliteratorSteam(Spliterator<E> impl) {
		super(impl);
	}

	@Override
	public <R> Steam<R> map(Function<E, R> conv) {
		return Steam.wrap(Steams.map(impl, conv));
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
		// TODO Auto-generated method stub
		return null;
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
