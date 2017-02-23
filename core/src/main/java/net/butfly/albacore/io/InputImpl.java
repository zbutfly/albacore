package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;

public abstract class InputImpl<V> extends Namedly implements Input<V>, Supplier<V>, Iterator<V> {

	protected InputImpl() {
		super();
	}

	protected InputImpl(String name) {
		super(name);
	}

	protected V dequeue() {
		return dequeue(1).findFirst().orElse(null);
	}

	@Override
	public Stream<V> dequeue(long batchSize) {
		return Streams.fetch(batchSize, () -> dequeue(), () -> empty(), null, true);
	}

	@Override
	public V get() {
		return dequeue();
	}

	@Override
	public boolean hasNext() {
		return !empty();
	}

	@Override
	public V next() {
		return dequeue();
	}
}
