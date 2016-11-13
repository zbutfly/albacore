package net.butfly.albacore.io;

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

public class PumpBase<V> implements Pump {
	protected final AbstractQueue<?, V> source;
	protected final AbstractQueue<V, ?> destination;
	protected AtomicBoolean running;
	final static UncaughtExceptionHandler DEFAULT_HANDLER = (t, e) -> {
		logger.error("Pump failure in one line [" + t.getName() + "]", e);
	};

	protected final ListeningExecutorService ex;
	protected final List<Thread> threads = new ArrayList<>();
	protected List<ListenableFuture<?>> futures = new ArrayList<>();

	boolean stopping() {
		return !running.get();
	};

	PumpBase(AbstractQueue<?, V> source, AbstractQueue<V, ?> destination, int parallelism, Runnable... step) {
		this(source, destination, parallelism, DEFAULT_HANDLER, step);
	}

	PumpBase(AbstractQueue<?, V> source, AbstractQueue<V, ?> destination, int parallelism, UncaughtExceptionHandler handler,
			Runnable... step) {
		Reflections.noneNull("", source, destination);
		this.source = source;
		this.destination = destination;
		running = new AtomicBoolean(false);
		StringBuilder name = new StringBuilder(source.name()).append(">>>").append(destination.name()).append("[");
		Runnable r = Concurrents.until(this::stopping, step);
		for (int i = 0; i < parallelism; i++) {
			Thread t = new Thread(r, name.append(i).append("]").toString());
			if (null != handler && t.getThreadGroup().equals(t.getUncaughtExceptionHandler())) t.setUncaughtExceptionHandler(handler);
			threads.add(t);
		}
		ex = Concurrents.executor(parallelism, source.name(), destination.name());
	}

	@Override
	public Pump start() {
		running.set(true);
		for (Thread t : threads)
			futures.add(ex.submit(t));
		return this;
	}

	@Override
	public Pump waiting() {
		try {
			Futures.successfulAsList(futures).get();
		} catch (InterruptedException | ExecutionException e) {
			logger.error("Pump waiting failure", e);
		}
		return this;
	}

	@Override
	public Pump waiting(long timeout, TimeUnit unit) {
		try {
			Futures.successfulAsList(futures).get(timeout, unit);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			logger.error("Pump waiting failure", e);
		}
		return this;
	}

	@Override
	public Pump stop() {
		Futures.successfulAsList(futures).cancel(true);
		ex.shutdown();
		Concurrents.waitShutdown(ex, logger);
		return this;
	}
}
