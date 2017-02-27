package net.butfly.albacore.io;

import static net.butfly.albacore.utils.Exceptions.unwrap;
import static net.butfly.albacore.utils.Exceptions.wrap;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.ListenableFuture;

import net.butfly.albacore.base.Namedly;

public class FanOutput<V> extends Namedly implements Output<V> {
	private final Iterable<? extends Output<V>> outputs;

	public FanOutput(Iterable<? extends Output<V>> outputs) {
		this("FanOutTo" + ":" + IO.collect(Streams.of(outputs).map(o -> o.name()), Collectors.joining("&")), outputs);
		open();
	}

	public FanOutput(String name, Iterable<? extends Output<V>> outputs) {
		super(name);
		this.outputs = outputs;
	}

	@Override
	public long enqueue(Stream<V> items) {
		List<V> values = IO.list(items);
		ListenableFuture<List<Long>> fs = IO.listen(IO.list(outputs, o -> () -> o.enqueue(Streams.of(values))));
		List<Long> rs;
		try {
			rs = fs.get();
		} catch (InterruptedException e) {
			throw new RuntimeException("Streaming inturrupted", e);
		} catch (Exception e) {
			throw wrap(unwrap(e));
		}
		return IO.collect(rs, Collectors.summingLong(Long::longValue));
	}
}
