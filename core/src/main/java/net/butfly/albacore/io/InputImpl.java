package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.function.Function;
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

	protected abstract V dequeue();

	@Override
	public final long dequeue(Function<Stream<V>, Long> using, long batchSize) {
		return using.apply(Streams.of(() -> dequeue(), batchSize, () -> empty() && opened()));
	}

	@Override
	public V get() {
		return dequeue();
	}
}
