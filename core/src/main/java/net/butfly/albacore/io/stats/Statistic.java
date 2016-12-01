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
		this.packsStep = new AtomicLong(step - 1);
		packsInStep = new AtomicLong(0);
		bytesInStep = new AtomicLong(0);
		packsInTotal = new AtomicLong(0);
		bytesInTotal = new AtomicLong(0);
		begin = new Date().getTime();
		statsed = new AtomicLong(begin);
		this.sizing = sizing;
		this.current = current;
	}

	void stats(long bytes) {
		packsInTotal.incrementAndGet();
		bytesInTotal.addAndGet(bytes < 0 ? 0 : bytes);
		bytesInStep.addAndGet(bytes < 0 ? 0 : bytes);
		if (packsInStep.incrementAndGet() > packsStep.get() && logger.isTraceEnabled()) trace();
	}

	void stats(V v) {
		stats(sizing.apply(v));
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
		logger.trace("Statistic: [Step: " + step.packs + "objs/" + formatBytes(step.bytes) + "/" + formatMillis(step.millis)
				+ "], [Total: " + total.packs + "objs/" + formatBytes(total.bytes) + "/" + formatMillis(total.millis) + "], [current: "
				+ current.get() + "].");
	}

	private static DecimalFormat f = new DecimalFormat("#.##");

	private static long K = 1024;
	private static long M = K * K;
	private static long G = M * K;
	private static long T = G * K;

	String formatBytes(long bytes) {
		double b = bytes;
		if (b > T) return f.format(b / T) + "TB";
		// +"+" + formatBytes(bytes % T);
		if (b > G) return f.format(b / G) + "GB";
		// +"+" + formatBytes(bytes % G);
		if (b > M) return f.format(b / M) + "MB";
		// +"+" + formatBytes(bytes % M);
		if (b > K) return f.format(b / K) + "KB";
		// +"+" + formatBytes(bytes % K);
		return f.format(b) + "Bytes";
	}

	private static int SECOND = 1000;
	private static int MINUTE = 60 * SECOND;
	private static int HOUR = 60 * MINUTE;

	String formatMillis(long millis) {
		double ms = millis;
		if (ms > HOUR) return f.format(ms / HOUR) + "h";
		// + "+" + formatMillis(millis % HOUR);
		if (ms > MINUTE) return f.format(ms / MINUTE) + "m";
		// + "+" + formatMillis(millis % MINUTE);
		if (ms > SECOND) return f.format(millis / SECOND) + "s";
		// + "+" + formatMillis(millis % SECOND);
		return f.format(ms) + "ms";
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