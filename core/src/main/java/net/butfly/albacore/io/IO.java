package net.butfly.albacore.io;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Stream;

import net.butfly.albacore.base.Sizable;

public interface IO extends Sizable, Openable {
	final int IO_PARALLELISM = Integer.parseInt(System.getProperty("albacore.io.parallenism", "16"));
	final StreamExecutor io = new StreamExecutor(IO_PARALLELISM, "AlbacoreIOExecutor", false);

	long size();

	static <V, A, R> R map(Iterable<V> col, Function<V, A> mapper, Collector<? super A, ?, R> collector) {
		return io.map(col, mapper, collector);
	}

	static <V, A, R> R collect(Iterable<V> col, Function<Stream<V>, Stream<A>> mapping, Collector<? super A, ?, R> collector) {
		return io.collect(col, mapping, collector);
	}

	static <V, R> R collect(Iterable<? extends V> col, Collector<V, ?, R> collector) {
		return io.collect(col, collector);
	}

	static <V, R> R collect(Stream<? extends V> stream, Collector<V, ?, R> collector) {
		return io.collect(stream, collector);
	}

	static <V> List<V> list(Stream<V> stream) {
		return io.list(stream);
	}

	static <V, R> List<R> list(Iterable<V> col, Function<V, R> mapper) {
		return io.list(col, mapper);
	}

	static <V> void each(Iterable<V> col, Consumer<? super V> consumer) {
		io.each(col, consumer);
	}
}
