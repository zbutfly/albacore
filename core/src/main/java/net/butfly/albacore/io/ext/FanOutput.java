package net.butfly.albacore.io.ext;

import static net.butfly.albacore.io.utils.Streams.collect;
import static net.butfly.albacore.io.utils.Streams.list;
import static net.butfly.albacore.io.utils.Streams.of;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.utils.parallel.Task;

public class FanOutput<V> extends Namedly implements Output<V> {
	private final Iterable<? extends Output<V>> outputs;

	public FanOutput(Iterable<? extends Output<V>> outputs) {
		this("FanOutTo" + ":" + collect(of(outputs).map(o -> o.name()), Collectors.joining("&")), outputs);
		open();
	}

	public FanOutput(String name, Iterable<? extends Output<V>> outputs) {
		super(name);
		this.outputs = outputs;
	}

	@Override
	public long enqueue(Stream<V> items) {
		List<V> values = list(items);
		AtomicLong c = new AtomicLong();
		Task t = null;
		for (Output<V> o : outputs) {
			Task t0 = () -> c.addAndGet(o.enqueue(of(values)));
			if (null == t) t = t0;
			else t = t.multiple(t0);
		}
		t.run();
		return c.get();
	}
}
