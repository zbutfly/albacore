package net.butfly.albacore.paral.split;

import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

abstract class PooledSpliteratorBase<E, E0> extends ConvedSpliteratorBase<E, E0> {
	protected final BlockingQueue<E> pool = new LinkedBlockingQueue<>();

	protected PooledSpliteratorBase(Spliterator<E0> impl, int ch) {
		super(impl, ch);
	}
}
