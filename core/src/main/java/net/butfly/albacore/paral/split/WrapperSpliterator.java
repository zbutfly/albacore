package net.butfly.albacore.paral.split;

import java.util.Objects;
import java.util.Spliterator;

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
}
