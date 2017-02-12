package net.butfly.albacore.io;

import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

public class FanOutput<V> extends OutputImpl<V> {
	private final Iterable<? extends Output<V>> outputs;

	public FanOutput(Iterable<? extends Output<V>> outputs) {
		this("FanOutTo" + ":" + io.collect(Streams.of(outputs).map(o -> o.name()), Collectors.joining("&")), outputs);
		open();
	}

	public FanOutput(String name, Iterable<? extends Output<V>> outputs) {
		super(name);
		this.outputs = outputs;
	}

	@Override
	public boolean enqueue(V item) {
		if (null == item) return false;
		for (Output<V> o : outputs)
			ForkJoinPool.commonPool().submit(() -> o.enqueue(item));
		return true;
	}
}
