package net.butfly.albacore.io;

import java.util.concurrent.ForkJoinPool;

public class FanOutput<V> extends OutputImpl<V> implements Output<V> {
	private final Iterable<? extends Output<V>> outputs;

	public FanOutput(Iterable<? extends Output<V>> outputs) {
		this("FanOutTo" + joinName(outputs), outputs);
	}

	private static <V> String joinName(Iterable<? extends Output<V>> outputs) {
		StringBuilder sb = new StringBuilder(":");
		for (Output<V> o : outputs)
			sb.append(o.name());
		return sb.toString();
	}

	public FanOutput(String name, Iterable<? extends Output<V>> outputs) {
		super(name);
		this.outputs = outputs;
	}

	@Override
	public boolean enqueue(V item) {
		for (Output<V> o : outputs)
			ForkJoinPool.commonPool().submit(() -> o.enqueue(item));
		return true;
	}
}
