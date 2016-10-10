package net.butfly.albacore.io;

import java.io.Closeable;
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

import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

public final class Pump<V> implements Closeable {
	private final static Logger logger = Logger.getLogger(Pump.class);
	private final int parallelism;
	private final AbstractQueue<?, V> source;
	private final AbstractQueue<V, ?> destination;
	private final List<Thread> threads = new ArrayList<>();
	private final UncaughtExceptionHandler handler;

	private ListeningExecutorService ex;
	private ListenableFuture<List<Object>> futures;
	private AtomicBoolean running;
	private final List<Runnable> intervals;

	Pump(int parallelism, AbstractQueue<?, V> source, AbstractQueue<V, ?> destination) {
		this(parallelism, source, destination, (t, e) -> {
			logger.error("Pump failure in one line [" + t.getName() + "]", e);
		});
	}

	Pump(int parallelism, AbstractQueue<?, V> source, AbstractQueue<V, ?> destination, UncaughtExceptionHandler handler) {
		running = new AtomicBoolean(false);
		Reflections.noneNull("", source, destination, handler);
		this.parallelism = parallelism;
		this.source = source;
		this.destination = destination;
		this.handler = handler;
		this.intervals = new ArrayList<>();
	}

	public void interval(Runnable interval) {
		this.intervals.add(interval);
	}

	void submit(Runnable r, int parallelism, String... nameSuffix) {
		StringBuilder nb = new StringBuilder(source.name()).append("->").append(destination.name());
		for (String s : nameSuffix)
			nb.append("-").append(s);
		nb.append("[");
		String name = nb.toString();
		for (int i = 0; i < parallelism; i++)
			threads.add(new Thread(Concurrents.forever(r, intervals.toArray(new Runnable[intervals.size()])), name + i + "]"));
	}

	public void start() {
		if (threads.isEmpty()) throw new RuntimeException("Pump start failure: nothing to pump");
		if (running.getAndSet(true)) new RuntimeException("Pump start failure: already pumping");
		ex = Concurrents.executor(parallelism, source.name(), destination.name());
		List<ListenableFuture<?>> fs = new ArrayList<>();
		for (Thread t : threads) {
			if (null != handler && t.getThreadGroup().equals(t.getUncaughtExceptionHandler())) t.setUncaughtExceptionHandler(handler);
			fs.add(ex.submit(t));
		}
		futures = Futures.allAsList(fs);
	}

	void startAndWait() {
		try {
			start();
			futures.get();
		} catch (InterruptedException | ExecutionException e) {
			logger.error("Pump waiting failure", e);
		}
	}

	void startAndWait(long timeout, TimeUnit unit) {
		try {
			start();
			futures.get(timeout, unit);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			logger.error("Pump waiting failure", e);
		}
	}

	@Override
	public void close() {
		if (!running.getAndSet(false)) new RuntimeException("Pump close failure: not in pumping, not started or close already");
		try {
			source.close();
			destination.close();
			futures.cancel(true);
			ex.shutdown();
			Concurrents.waitShutdown(ex, logger);
		} catch (IOException e) {
			logger.error("Close failure", e);
		} finally {
			ex = null;
		}
	}

}
