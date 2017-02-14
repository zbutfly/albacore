package net.butfly.albacore.io;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
		boolean r = true;
		List<Boolean> rs = io.submit(io.list(outputs, o -> () -> o.enqueue(item)));
		for (Boolean b : rs)
			r = r && b;
		return r;
	}

	@Override
	public long enqueue(Stream<V> items) {
		return io.collect(io.submit(io.list(outputs, o -> () -> o.enqueue(items))), Collectors.summingLong(Long::longValue));
	}
}
