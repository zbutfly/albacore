package net.butfly.albacore.io.pump;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import net.butfly.albacore.io.Queue;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.async.Concurrents;

public class PumpBase<V> implements Pump<V> {
	private static final long serialVersionUID = 663917114528791086L;
	protected final Queue<?, V> source;
	protected final Queue<V, ?> destination;
	protected AtomicBoolean running;
	final static UncaughtExceptionHandler DEFAULT_HANDLER = (t, e) -> {
		logger.error("Pump failure in one line [" + t.getName() + "]", e);
	};

	protected final ListeningExecutorService ex;
	protected final List<Thread> threads = new ArrayList<>();
	protected List<ListenableFuture<?>> futures = new ArrayList<>();
	private long batchSize = 1000;

	public boolean stopped() {
		return !running.get();
	}

	@Override
	public Pump<V> batch(long batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	public PumpBase(Queue<?, V> source, Queue<V, ?> destination, int parallelism) {
		this(source, destination, parallelism, DEFAULT_HANDLER);
	}

	public PumpBase(Queue<?, V> source, Queue<V, ?> destination, int parallelism, UncaughtExceptionHandler handler) {
		Reflections.noneNull("", source, destination);
		this.source = source;
		this.destination = destination;
		running = new AtomicBoolean(false);
		StringBuilder name = new StringBuilder(source.name()).append(">>>").append(destination.name()).append("[");
		Runnable r = Concurrents.until(this::stopped, () -> {
			destination.enqueue(stats(source.dequeue(batchSize)));
		});
		for (int i = 0; i < parallelism; i++) {
			Thread t = new Thread(r, name.append(i).append("]").toString());
			if (null != handler && t.getThreadGroup().equals(t.getUncaughtExceptionHandler())) t.setUncaughtExceptionHandler(handler);
			threads.add(t);
		}
		ex = Concurrents.executor(parallelism, source.name(), destination.name());
	}

	@Override
	public Pump<V> start() {
		running.set(true);
		for (Thread t : threads)
			futures.add(ex.submit(t));
		return this;
	}

	@Override
	public Pump<V> waiting() {
		try {
			Futures.successfulAsList(futures).get();
		} catch (InterruptedException | ExecutionException e) {
			logger.error("Pump waiting failure", e);
		}
		return this;
	}

	@Override
	public Pump<V> waiting(long timeout, TimeUnit unit) {
		try {
			Futures.successfulAsList(futures).get(timeout, unit);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			logger.error("Pump waiting failure", e);
		}
		return this;
	}

	@Override
	public Pump<V> stop() {
		Futures.successfulAsList(futures).cancel(true);
		ex.shutdown();
		Concurrents.waitShutdown(ex, logger);
		return this;
	}
}
