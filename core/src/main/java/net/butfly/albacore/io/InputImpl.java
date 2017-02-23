package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.List;
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
		List<V> l = IO.list(dequeue(1));
		return null == l || l.isEmpty() ? null : l.get(0);
	}

	@Override
	public Stream<V> dequeue(long batchSize) {
		return Streams.of(Streams.batch(batchSize, () -> dequeue(), () -> empty()));
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
