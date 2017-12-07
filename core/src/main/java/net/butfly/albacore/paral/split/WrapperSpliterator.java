package net.butfly.albacore.paral.split;

import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicReference;

abstract class WrapperSpliterator<E> extends LockSpliterator<E> {
	protected final Spliterator<E> impl;

	protected WrapperSpliterator(Spliterator<E> impl) {
		super();
		this.impl = Objects.requireNonNull(impl);
	}

	@Override
	public int characteristics() {
		return read(() -> impl.characteristics());
	}

	@Override
	public long estimateSize() {
		return impl.estimateSize();
	}

	public Optional<E> next() {
		return (characteristics() & Spliterator.CONCURRENT) != 0 ? doNext() : write(this::doNext);
	}

	private Optional<E> doNext() {
		AtomicReference<E> ref = new AtomicReference<>();
		return impl.tryAdvance(ref::lazySet) ? Optional.ofNullable(ref.get()) : null;
	}
}
