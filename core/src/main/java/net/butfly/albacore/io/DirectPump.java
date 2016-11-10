package net.butfly.albacore.io;

import java.io.IOException;
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

import net.butfly.albacore.lambda.Supplier;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.async.Concurrents;

public class DirectPump<V> implements Pump {
	private final int parallelism;
	private final AbstractQueue<?, V> source;
	private final AbstractQueue<V, ?> destination;
	private final List<Thread> threads = new ArrayList<>();
	private final UncaughtExceptionHandler handler;

	private ListeningExecutorService ex;
	private ListenableFuture<List<Object>> future;
	private AtomicBoolean running;
	private final List<Runnable> intervals;

	DirectPump(AbstractQueue<?, V> source, AbstractQueue<V, ?> destination, int parallelism) {
		this(source, destination, parallelism, DEFAULT_HANDLER);
	}

	DirectPump(AbstractQueue<?, V> source, AbstractQueue<V, ?> destination, int parallelism, UncaughtExceptionHandler handler) {
		running = new AtomicBoolean(false);
		Reflections.noneNull("", source, destination, handler);
		this.parallelism = parallelism;
		this.source = source;
		this.destination = destination;
		this.handler = handler;
		this.intervals = new ArrayList<>();
	}

	@Override
	public Pump interval(Runnable interval) {
		intervals.add(interval);
		return this;
	}

	@Override
	public Pump submit(Supplier<Boolean> stopping, int parallelism, String... nameSuffix) {
		StringBuilder nb = new StringBuilder(source.name()).append("->").append(destination.name());
		for (String s : nameSuffix)
			nb.append("-").append(s);
		nb.append("[");
		String name = nb.toString();
		for (int i = 0; i < parallelism; i++)
			threads.add(new Thread(Concurrents.until(stopping, intervals.toArray(new Runnable[intervals.size()])), name + i + "]"));
		return this;
	}

	@Override
	public Pump start() {
		if (threads.isEmpty()) throw new RuntimeException("Pump start failure: nothing to pump");
		if (running.getAndSet(true)) new RuntimeException("Pump start failure: already pumping");
		ex = Concurrents.executor(parallelism, source.name(), destination.name());
		List<ListenableFuture<?>> fs = new ArrayList<>();
		for (Thread t : threads) {
			if (null != handler && t.getThreadGroup().equals(t.getUncaughtExceptionHandler())) t.setUncaughtExceptionHandler(handler);
			fs.add(ex.submit(t));
		}
		future = Futures.successfulAsList(fs);
		return this;
	}

	@Override
	public Pump waiting() {
		try {
			future.get();
		} catch (InterruptedException | ExecutionException e) {
			logger.error("Pump waiting failure", e);
		}
		return this;
	}

	@Override
	public Pump waiting(long timeout, TimeUnit unit) {
		try {
			future.get(timeout, unit);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			logger.error("Pump waiting failure", e);
		}
		return this;
	}

	@Override
	public Pump terminate() {
		if (!running.getAndSet(false)) new RuntimeException("Pump close failure: not in pumping, not started or close already");
		try {
			source.close();
			destination.close();
			future.cancel(true);
			if (null != ex) ex.shutdown();
			Concurrents.waitShutdown(ex, logger);
		} catch (IOException e) {
			logger.error("Close failure", e);
		} finally {
			ex = null;
		}
		return this;
	}

	@Override
	public Pump terminate(long timeout, TimeUnit unit) {
		if (!running.getAndSet(false)) new RuntimeException("Pump close failure: not in pumping, not started or close already");
		try {
			future.cancel(true);
			if (null != ex) ex.shutdown();
			Concurrents.waitShutdown(ex, logger);
		} finally {
			ex = null;
		}
		return this;
	}
}
