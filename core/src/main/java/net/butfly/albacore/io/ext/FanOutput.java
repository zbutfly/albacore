package net.butfly.albacore.io.ext;

import static net.butfly.albacore.io.utils.Streams.collect;
import static net.butfly.albacore.io.utils.Streams.list;
import static net.butfly.albacore.io.utils.Streams.of;
import static net.butfly.albacore.utils.Exceptions.unwrap;
import static net.butfly.albacore.utils.Exceptions.wrap;
import static net.butfly.albacore.utils.parallel.Parals.listen;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.ListenableFuture;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.io.Output;

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
		ListenableFuture<List<Long>> fs = listen(list(outputs, o -> () -> o.enqueue(of(values))));
		List<Long> rs;
		try {
			rs = fs.get();
		} catch (InterruptedException e) {
			throw new RuntimeException("Streaming inturrupted", e);
		} catch (Exception e) {
			throw wrap(unwrap(e));
		}
		return collect(rs, Collectors.summingLong(Long::longValue));
	}
}
