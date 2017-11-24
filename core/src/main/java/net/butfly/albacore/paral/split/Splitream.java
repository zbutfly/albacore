package net.butfly.albacore.paral.split;

import static net.butfly.albacore.paral.split.SplitEx.concat;
import static net.butfly.albacore.paral.steam.Steam.of;

import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import net.butfly.albacore.paral.steam.Steam;
import net.butfly.albacore.utils.Pair;

public final class Splitream<E, SELF extends Steam<E>> implements Steam<E>, Spliterator<E> {
	protected final Spliterator<E> impl;

	public Splitream(Spliterator<E> impl) {
		super();
		this.impl = Objects.requireNonNull(impl);
	}

	@Override
	public Spliterator<E> spliterator() {
		return impl;
	}

	// =======================================

	@Override
	public Steam<E> filter(Predicate<E> checking) {
		return of(new Spliterator<E>() {
			@Override
			public int characteristics() {
				return impl.characteristics();
			}

			@Override
			public long estimateSize() {
				return impl.estimateSize();
			}

			@Override
			public boolean tryAdvance(Consumer<? super E> using) {
				AtomicBoolean d = new AtomicBoolean(false);
				while (!d.get())
					if (!impl.tryAdvance(e -> {
						boolean u = null != e;
						d.lazySet(u);
						if (u) using.accept(e);
					})) return false;
				return d.get();
			}

			@Override
			public Spliterator<E> trySplit() {
				Spliterator<E> ss = impl.trySplit();
				return null == ss ? null : of(ss).filter(checking).spliterator();
			}
		});
	}

	@Override
	public <R> Steam<R> map(Function<E, R> conv) {
		return of(SplitEx.map(impl, conv));
	}

	@Override
	public <R> Steam<R> map(Function<Steam<E>, Steam<R>> conv, int maxBatchSize) {
		return of(SplitEx.map(impl, conv, maxBatchSize));
	}

	@Override
	public <R> Steam<R> mapFlat(Function<E, Steam<R>> flat) {
		return of(SplitEx.mapFlat(impl, flat));
	}

	@Override
	public E reduce(BinaryOperator<E> accumulator) {
		return SplitEx.reduce(impl, accumulator);
	}

	@Override
	public <R> Steam<Pair<E, R>> join(Function<E, R> joining) {
		return of(SplitEx.join(impl, joining));
	}

	@Override
	public Steam<E> union(Steam<E> another) {
		return of(concat(impl, another.spliterator()));
	}

	@Override
	public
	// ==================
	List<E> list() {
		return SplitEx.list(impl);
	}

	@Override
	public void each(Consumer<E> using) {
		SplitEx.each(impl, using);
	}

	@Override
	public void batch(Consumer<Steam<E>> using, int maxBatchSize) {
		SplitEx.batch(impl, using, maxBatchSize);
	}

	@Override
	public void partition(Consumer<Steam<E>> using, int minPartNum) {
		SplitEx.partition(impl, using, minPartNum);
	}

	@Override
	public <K> void partition(BiConsumer<K, E> using, Function<E, K> keying) {
		SplitEx.partition(impl, using, keying);
	}

	@Override
	public <K> void partition(BiConsumer<K, Steam<E>> using, Function<E, K> keying, int maxBatchSize) {
		SplitEx.partition(impl, using, keying, maxBatchSize);
	}

	// =========================

	@Override
	public int characteristics() {
		return impl.characteristics();
	}

	@Override
	public long estimateSize() {
		return impl.estimateSize();
	}

	@Override
	public boolean tryAdvance(Consumer<? super E> using) {
		return impl.tryAdvance(using);
	}

	@Override
	public Spliterator<E> trySplit() {
		return impl.trySplit();
	}

	@Override
	public <E1> Steam<Pair<E, E1>> join(Function<Steam<E>, Steam<E1>> joining, int maxBatchSize) {
		return null;
	}
}
