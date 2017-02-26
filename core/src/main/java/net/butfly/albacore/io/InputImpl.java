package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
		AtomicReference<V> ref = new AtomicReference<>();
		dequeue(s -> ref.lazySet(s.findFirst().orElse(null)), 1);
		return ref.get();
	}

	@Override
	public void dequeue(Consumer<Stream<V>> using, long batchSize) {
		using.accept(Streams.of(() -> dequeue(), batchSize, () -> empty()));
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
