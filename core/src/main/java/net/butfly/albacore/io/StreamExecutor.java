package net.butfly.albacore.io;

import static net.butfly.albacore.utils.Exceptions.unwrap;
import static net.butfly.albacore.utils.Exceptions.wrap;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
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
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.logger.Logger;
import net.butfly.albacore.utils.parallel.Concurrents;

public final class StreamExecutor extends Namedly implements AutoCloseable {
	static final Logger logger = Logger.getLogger(StreamExecutor.class);
	final ExecutorService executor;
	final ListeningExecutorService lex;
	private final static Map<String, ThreadGroup> g = new ConcurrentHashMap<>();

	public StreamExecutor(String name, int parallelism, boolean throwException) {
		executor = parallelism < 1 ? Executors.newCachedThreadPool(r -> new Thread(g.computeIfAbsent(name, n -> new ThreadGroup(name
				+ "-ThreadGroup")), r, name + "@" + Texts.formatDate(new Date())))
				: Concurrents.executorForkJoin(parallelism, name, (t, e) -> {
					logger.error("Migrater pool task failure @" + t.getName(), e);
					if (throwException) throw wrap(unwrap(e));
				});
		if (executor instanceof ThreadPoolExecutor) ((ThreadPoolExecutor) executor).setRejectedExecutionHandler((r, ex) -> logger.error(
				tracePool("Task rejected by the executor")));
		lex = MoreExecutors.listeningDecorator(executor);
	}

	public void run(Runnable... tasks) {
		get(listenRun(tasks));
	}

	public void run(Runnable task) {
		get(listenRun(task));
	}

	public <T> T run(Callable<T> task) {
		return get(listen(task));
	}

	public <T> List<T> run(List<Callable<T>> tasks) {
		return get(listen(tasks));
	}

	public <T> ListenableFuture<List<T>> listen(List<? extends Callable<T>> tasks) {
		return Futures.successfulAsList(list(tasks, this::listen));
	}

	public <T> ListenableFuture<T> listen(Callable<T> task) {
		try {
			return lex.submit(task);
		} catch (RejectedExecutionException e) {
			logger.error("Rejected");
			throw e;
		}
	}

	public ListenableFuture<List<Object>> listenRun(Runnable... tasks) {
		return Futures.successfulAsList(list(Arrays.asList(tasks), this::listenRun));
	}

	public ListenableFuture<?> listenRun(Runnable task) {
		try {
			return lex.submit(task);
		} catch (RejectedExecutionException e) {
			logger.error("Rejected");
			throw e;
		}
	}

	public <V, A, R> R collect(Iterable<V> col, Function<V, A> mapper, Collector<? super A, ?, R> collector) {
		return collect(Streams.of(col).map(mapper), collector);
	}

	public <T, T1, K, V> Map<K, V> map(Stream<T> col, Function<T, T1> mapper, Function<T1, K> keying, Function<T1, V> valuing) {
		return collect(col.map(mapper), Collectors.toMap(keying, valuing));
	}

	public <T, K, V> Map<K, V> map(Stream<T> col, Function<T, K> keying, Function<T, V> valuing) {
		return collect(col, Collectors.toMap(keying, valuing));
	}

	public <V, A, R> R mapping(Iterable<V> col, Function<Stream<V>, Stream<A>> mapping, Collector<? super A, ?, R> collector) {
		return collect(mapping.apply(Streams.of(col)), collector);
	}

	public <V, R> R collect(Iterable<? extends V> col, Collector<? super V, ?, R> collector) {
		return collect(Streams.of(col), collector);
	}

	public <V, R> R collect(Stream<? extends V> s, Collector<? super V, ?, R> collector) {
		if (!logger.isTraceEnabled()) return Streams.of(s).collect(collector);
		AtomicLong c = new AtomicLong();
		R r = Streams.of(s).peek(e -> c.incrementAndGet()).collect(collector);
		logger.debug("One stream collected [" + c.get() + "] elements, performance issue?");
		return r;
	}

	public <V> List<V> list(Stream<? extends V> stream) {
		return collect(Streams.of(stream), Collectors.toList());
	}

	public <V, R> List<R> list(Iterable<V> col, Function<V, R> mapper) {
		return collect(col, mapper, Collectors.toList());
	}

	public <V> void each(Iterable<V> col, Consumer<? super V> consumer) {
		each(Streams.of(col), consumer);
	}

	private <V> void each(Stream<V> s, Consumer<? super V> consumer) {
		get(Futures.successfulAsList(list(Streams.of(s).map(v -> listenRun(() -> consumer.accept(v))))));
	}

	public String tracePool(String prefix) {
		if (executor instanceof ForkJoinPool) {
			ForkJoinPool ex = (ForkJoinPool) executor;
			return MessageFormat.format("{5}, Fork/Join: tasks={4}, threads(active/running)={1}/{2}, steals={3}, pool size={0}", ex
					.getPoolSize(), ex.getActiveThreadCount(), ex.getRunningThreadCount(), ex.getStealCount(), ex.getQueuedTaskCount(),
					prefix);
		} else if (executor instanceof ThreadPoolExecutor) {
			ThreadPoolExecutor ex = (ThreadPoolExecutor) executor;
			return MessageFormat.format("{3}, ThreadPool: tasks={2}, threads(active)={1}, pool size={0}", ex.getPoolSize(), ex
					.getActiveCount(), ex.getTaskCount(), prefix);
		} else return prefix + ": " + executor.toString();
	}

	@Override
	public void close() {
		// Concurrents.shutdown(executor);
	}

	public int parallelism() {
		return executor instanceof ForkJoinPool ? ((ForkJoinPool) executor).getParallelism() : 0;
	}

	private <T> T get(Future<T> f) {
		try {
			return f.get();
		} catch (InterruptedException e) {} catch (ExecutionException e) {
			logger.error("Subtask error", unwrap(e));
		}
		return null;
	}
}