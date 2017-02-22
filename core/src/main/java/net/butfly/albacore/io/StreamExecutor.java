package net.butfly.albacore.io;

import static net.butfly.albacore.utils.Exceptions.unwrap;
import static net.butfly.albacore.utils.Exceptions.wrap;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import net.butfly.albacore.base.Namedly;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

public final class StreamExecutor extends Namedly implements AutoCloseable {
	private static final Logger logger = Logger.getLogger(StreamExecutor.class);
	private final ForkJoinPool executor;
	private final ListeningExecutorService lex;

	public StreamExecutor(int parallelism, String threadNamePrefix, boolean throwException) {
		if (parallelism < 1) executor = ForkJoinPool.commonPool();
		else executor = Concurrents.executorForkJoin(parallelism, "AlbatisIOPool", (t, e) -> {
			logger.error("Migrater pool task failure @" + t.getName(), e);
			if (throwException) throw wrap(unwrap(e));
		});
		lex = MoreExecutors.listeningDecorator(executor);
	}

	public void run(Runnable task) {
		for (int i = 1;; i++)
			try {
				executor.submit(task).get();
				return;
			} catch (RejectedExecutionException e) {
				logger.warn(tracePool("Rejected #" + i), e);
				Concurrents.waitSleep();
			} catch (InterruptedException e) {
				throw new RuntimeException("Streaming inturrupted", e);
			} catch (ExecutionException e) {
				throw wrap(unwrap(e));
			}
	}

	public <T> T run(Callable<T> task) {
		for (int i = 1;; i++)
			try {
				return executor.submit(task).get();
			} catch (RejectedExecutionException e) {
				logger.warn(tracePool("Rejected #" + i), e);
				Concurrents.waitSleep();
			} catch (InterruptedException e) {
				throw new RuntimeException("Streaming inturrupted", e);
			} catch (Exception e) {
				throw wrap(unwrap(e));
			}
	}

	public <T> List<T> run(@SuppressWarnings("unchecked") Callable<T>... tasks) {
		for (int i = 1;; i++)
			try {
				return listen(Arrays.asList(tasks)).get();
			} catch (RejectedExecutionException e) {
				logger.warn(tracePool("Rejected #" + i), e);
				Concurrents.waitSleep();
			} catch (InterruptedException e) {
				throw new RuntimeException("Streaming inturrupted", e);
			} catch (Exception e) {
				throw wrap(unwrap(e));
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

	public <V> void each(Iterable<V> col, Consumer<? super V> consumer) {
		run(() -> Streams.of(col).forEach(consumer));
	}

	public <V> void each(Stream<V> of, Consumer<? super V> consumer) {
		run(() -> Streams.of(of).forEach(consumer));
	}

	public <T> ListenableFuture<List<T>> listen(List<? extends Callable<T>> list) {
		return Futures.successfulAsList(list(list, c -> lex.submit(c)));
	}

	public ListenableFuture<List<Object>> listenRun(List<? extends Runnable> list) {
		return Futures.successfulAsList(list(list, c -> lex.submit(c)));
	}

	public String tracePool(String prefix) {
		return MessageFormat.format("{6}, Fork/Join: tasks={5}, threads(active/running)={2}/{3}, steals={4}, pool size={1}.", null, executor
				.getPoolSize(), executor.getActiveThreadCount(), executor.getRunningThreadCount(), executor.getStealCount(), executor
						.getQueuedTaskCount(), prefix);
	}

	@Override
	public void close() throws Exception {
		Concurrents.shutdown(executor);
	}

	public int parallelism() {
		return executor.getParallelism();
	}
}