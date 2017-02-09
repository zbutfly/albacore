package net.butfly.albacore.io;

import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.google.common.base.Joiner;

public class FanOutput<V> extends OutputImpl<V> implements Output<V> {
	private final Iterable<? extends Output<V>> outputs;

	public FanOutput(Iterable<? extends Output<V>> outputs) {
		this("FanOutTo" + new StringBuilder(":").append(Joiner.on('&').join(StreamSupport.stream(outputs.spliterator(), false).map(o -> o
				.name()).collect(Collectors.toList()))).toString().toString(), outputs);
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
