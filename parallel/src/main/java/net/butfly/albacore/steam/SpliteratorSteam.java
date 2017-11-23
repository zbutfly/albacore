package net.butfly.albacore.steam;

import static net.butfly.albacore.steam.SplitEx.concat;
import static net.butfly.albacore.steam.SplitEx.of;

import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;

import net.butfly.albacore.utils.Pair;

class SpliteratorSteam<E> extends SteamBase<E, Spliterator<E>, SpliteratorSteam<E>> implements Steam<E> {
	SpliteratorSteam(Spliterator<E> impl) {
		super(impl);
	}

	@Override
	public <R> Steam<R> map(Function<E, R> conv) {
		return of(SplitEx.map(impl, conv));
	}

	@Override
	public <R> Steam<R> mapFlat(Function<E, List<R>> flat) {
		return of(SplitEx.mapFlat(impl, flat));
	}

	@Override
	public E reduce(BinaryOperator<E> accumulator) {
		AtomicReference<E> r = new AtomicReference<>();
		SplitEx.each(impl, e -> r.accumulateAndGet(e, (e1, e2) -> {
			if (null == e1) return e2;
			if (null == e2) return e1;
			return accumulator.apply(e1, e2);
		}));
		return r.get();
	}

	@Override
	public <R> Steam<Pair<E, R>> join(Function<E, R> joining) {
		return of(SplitEx.join(impl, joining));
	}

	@Override
	public Steam<E> union(Steam<E> another) {
		return of(concat(impl, another.spliterator()));
	}

	// ==================

	@Override
	public Spliterator<E> spliterator() {
		return impl;
	}

	@Override
	public void each(Consumer<E> using) {
		SplitEx.each(impl, using);
	}

	@Override
	public boolean next(Consumer<E> using) {
		return impl.tryAdvance((Consumer<? super E>) using);
	}

	@Override
	public void batch(Consumer<List<E>> using, int maxBatchSize) {
		SplitEx.batch(impl, using, maxBatchSize);
	}

	@Override
	public void partition(Consumer<E> using, int minPartNum) {
		SplitEx.partition(impl, using, minPartNum);
	}

	@Override
	public <K> void partition(BiConsumer<K, List<E>> using, Function<E, K> keying, int maxBatchSize) {
		SplitEx.partition(impl, using, keying, maxBatchSize);
	}
}
