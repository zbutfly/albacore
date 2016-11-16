package net.butfly.albacore.io.stats;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.Supplier;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.logger.Logger;

class Statistic<T extends Statistical<T, V>, V> implements Serializable {
	private static final long serialVersionUID = -1;
	final StampedLock lock;

	final Logger logger;
	final AtomicLong packsStep;

	final long begin;
	final AtomicLong statsed;

	final AtomicLong packsInStep;
	final AtomicLong bytesInStep;
	final AtomicLong packsInTotal;
	final AtomicLong bytesInTotal;
	final Supplier<Long> current;
	final Converter<V, Long> sizing;

	Statistic(T owner, Logger logger, long step, Converter<V, Long> sizing, Supplier<Long> current) {
		Reflections.noneNull("", owner, logger);
		lock = new StampedLock();
		this.logger = logger;
		this.packsStep = new AtomicLong(step);
		packsInStep = new AtomicLong(0);
		bytesInStep = new AtomicLong(0);
		packsInTotal = new AtomicLong(0);
		bytesInTotal = new AtomicLong(0);
		begin = new Date().getTime();
		statsed = new AtomicLong(begin);
		this.sizing = sizing;
		this.current = current;
	}

	boolean stats(long bytes) {
		packsInTotal.incrementAndGet();
		bytesInTotal.addAndGet(bytes < 0 ? 0 : bytes);
		bytesInStep.addAndGet(bytes < 0 ? 0 : bytes);
		return packsInStep.incrementAndGet() > packsStep.get();
	}

	void stats(V v) {
		if (stats(sizing.apply(v)) && logger.isTraceEnabled()) trace();
	}

	void stats(Iterable<V> vv) {
		for (V v : vv)
			stats(v);
	}

	void trace() {
		long now = new Date().getTime();
		Statistic.Result step, total;
		step = new Statistic.Result(packsInStep.getAndSet(0), bytesInStep.getAndSet(0), now - statsed.getAndSet(now));
		total = new Statistic.Result(packsInTotal.get(), bytesInTotal.get(), new Date().getTime() - begin);
		logger.trace("Statistic:\n"//
				+ "\tStep: " + step.packs + " records/" + formatBytes(step.bytes) + " in " + formatMillis(step.millis) + "\n"//
				+ "\tTotal: " + total.packs + " records/" + formatBytes(total.bytes) + " in " + formatMillis(total.millis) + "\n"//
				+ "\tcurrent: " + current.get() + "\n"//
		);
	}

	private static DecimalFormat f = new DecimalFormat("#.##");

	private static double K = 1024;
	private static double M = K * K;
	private static double G = M * K;
	private static double T = G * K;

	String formatBytes(long bytes) {
		String bb;
		if (bytes > T * 0.8) bb = f.format(bytes / T) + "TB";
		else if (bytes > G * 0.8) bb = f.format(bytes / G) + "GB";
		else if (bytes > M * 0.8) bb = f.format(bytes / M) + "MB";
		else if (bytes > K * 0.8) bb = f.format(bytes / K) + "KB";
		else bb = f.format(bytes) + "Byte";
		return bb;
	}

	String formatMillis(long millis) {
		return f.format(millis / 1000.0) + "ms";
	}

	static class Result {
		public final long packs;
		public final long bytes;
		public final long millis;

		public Result(long packs, long bytes, long millis) {
			super();
			this.packs = packs;
			this.bytes = bytes;
			this.millis = millis;
		}
	}

}