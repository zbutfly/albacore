package net.butfly.albacore.io;

import static net.butfly.albacore.utils.Exceptions.unwrap;
import static net.butfly.albacore.utils.Exceptions.wrap;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

public class StreamExecutor extends Namedly implements AutoCloseable {
	private static final Logger logger = Logger.getLogger(StreamExecutor.class);
	private final ForkJoinPool executor;
	private final ListeningExecutorService lex;

	public StreamExecutor(int parallelism, String threadNamePrefix, boolean throwException) {
		if (parallelism <= 0) parallelism = Runtime.getRuntime().availableProcessors() / 2;
		if (parallelism <= 0) parallelism = 2;
		executor = Concurrents.executorForkJoin(parallelism, "AlbatisIOPool", (t, e) -> {
			logger.error("Migrater pool task failure @" + t.getName(), e);
			if (throwException) throw wrap(unwrap(e));
		});
		lex = MoreExecutors.listeningDecorator(executor);
	}

	void run(Runnable task) {
		ForkJoinTask<?> f = executor.submit(task);
		try {
			f.get();
		} catch (InterruptedException e) {
			throw new RuntimeException("Streaming inturrupted", e);
		} catch (ExecutionException e) {
			throw (e.getCause() instanceof RuntimeException) ? (RuntimeException) e.getCause()
					: new RuntimeException("Streaming failure", e.getCause());
		}
	}

	<T> T run(Callable<T> task) {
		ForkJoinTask<T> f = executor.submit(task);
		try {
			return f.get();
		} catch (InterruptedException e) {
			throw new RuntimeException("Streaming inturrupted", e);
		} catch (Exception e) {
			throw wrap(unwrap(e));
		}
	}

	<T> ForkJoinTask<T> submit(Callable<T> task) {
		return executor.submit(task);
	}

	public <T> List<T> submit(List<Callable<T>> list) {
		try {
			return Futures.successfulAsList(list(list, c -> lex.submit(c))).get();
		} catch (InterruptedException e) {
			throw new RuntimeException("Streaming inturrupted", e);
		} catch (Exception e) {
			throw wrap(unwrap(e));
		}
	}

	ForkJoinTask<?> submit(Runnable task) {
		return executor.submit(task);
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

	@Override
	public void close() throws Exception {
		Concurrents.shutdown(executor);
	}

	public <V> void each(Iterable<V> col, Consumer<? super V> consumer) {
		run(() -> Streams.of(col).forEach(consumer));
	}

	public <V> void each(Stream<V> of, Consumer<? super V> consumer) {
		run(() -> Streams.of(of).forEach(consumer));
	}

}