package net.butfly.albacore.io;

import static net.butfly.albacore.utils.Exceptions.unwrap;
import static net.butfly.albacore.utils.Exceptions.wrap;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

public class StreamExecutor extends Namedly {
	private static final Logger logger = Logger.getLogger(StreamExecutor.class);
	private ForkJoinPool executor;

	public StreamExecutor(int parallelism, String threadNamePrefix, boolean throwException) {
		if (parallelism <= 0) parallelism = Runtime.getRuntime().availableProcessors() / 2;
		if (parallelism <= 0) parallelism = 2;
		executor = Concurrents.executorForkJoin(parallelism, "AlbatisIOPool", (t, e) -> {
			logger.error("Migrater pool task failure @" + t.getName(), e);
			if (throwException) throw wrap(unwrap(e));
		});
	}

	private <T> T run(Callable<T> task) {
		ForkJoinTask<T> f = executor.submit(task);
		try {
			return f.get();
		} catch (InterruptedException e) {
			throw new RuntimeException("Streaming inturrupted", e);
		} catch (ExecutionException e) {
			throw (e.getCause() instanceof RuntimeException) ? (RuntimeException) e.getCause()
					: new RuntimeException("Streaming failure", e.getCause());
		}
	}

	public <V, A, R> R map(Iterable<V> col, Function<V, A> mapper, Collector<? super A, ?, R> collector) {
		return collect(Streams.of(col).map(mapper), collector);
	}

	public <V, A, R> R collect(Iterable<V> col, Function<Stream<V>, Stream<A>> mapping, Collector<? super A, ?, R> collector) {
		return collect(mapping.apply(Streams.of(col)), collector);
	}

	public <V, R> R collect(Iterable<? extends V> col, Collector<? super V, ?, R> collector) {
		return collect(Streams.of(col), collector);
	}

	public <V, R> R collect(Stream<? extends V> stream, Collector<? super V, ?, R> collector) {
		return run(() -> Streams.of(stream).collect(collector));
	}

	public <V> List<V> list(Stream<? extends V> stream) {
		return collect(stream, Collectors.toList());
	}

	public <V, R> List<R> list(Iterable<V> col, Function<V, R> mapper) {
		return map(col, mapper, Collectors.toList());
	}
}