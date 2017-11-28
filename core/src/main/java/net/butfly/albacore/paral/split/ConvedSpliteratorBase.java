package net.butfly.albacore.paral.split;

import java.util.Objects;
import java.util.Spliterator;

abstract class ConvedSpliteratorBase<E, E0> extends LockSpliterator<E> {
	protected final Spliterator<E0> impl;
	protected final int ch;

	protected ConvedSpliteratorBase(Spliterator<E0> impl, int ch) {
		super();
		this.impl = Objects.requireNonNull(impl);
		this.ch = impl.characteristics();
	}

	@Override
	public int characteristics() {
		return ch;
	}

	@Override
	public long estimateSize() {
		return impl.estimateSize();
	}
}
