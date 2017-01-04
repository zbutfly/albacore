package net.butfly.albacore.io.stats;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.Supplier;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.logger.Logger;

class Statistic implements Serializable {
	private static final long serialVersionUID = -1;
	final ReentrantLock lock;

	final Logger logger;
	final AtomicLong packsStep;

	final long begin;
	final AtomicLong statsed;

	final AtomicLong packsInStep;
	final AtomicLong bytesInStep;
	final AtomicLong packsInTotal;
	final AtomicLong bytesInTotal;
	final Supplier<String> suffixing;
	final Converter<Object, Long> sizing;

	<T extends Statistical<T>> Statistic(Statistical<T> owner, Logger logger, long step, Converter<Object, Long> sizing,
			Supplier<String> suffixing) {
		Reflections.noneNull("", owner, logger);
		lock = new ReentrantLock();
		this.logger = logger;
		this.packsStep = new AtomicLong(step - 1);
		packsInStep = new AtomicLong(0);
		bytesInStep = new AtomicLong(0);
		packsInTotal = new AtomicLong(0);
		bytesInTotal = new AtomicLong(0);
		begin = new Date().getTime();
		statsed = new AtomicLong(begin);
		this.sizing = sizing;
		this.suffixing = suffixing;
	}

	void stats(long bytes) {
		packsInTotal.incrementAndGet();
		bytesInTotal.addAndGet(bytes < 0 ? 0 : bytes);
		bytesInStep.addAndGet(bytes < 0 ? 0 : bytes);
		if (packsInStep.incrementAndGet() > packsStep.get() && logger.isTraceEnabled() && lock.tryLock()) try {
			trace();
		} finally {
			lock.unlock();
		}

	}

	void stats(Object v) {
		stats(sizing.apply(v));
	}

	void stats(Iterable<?> vv) {
		for (Object v : vv)
			stats(v);
	}

	void trace() {
		long now = new Date().getTime();
		Statistic.Result step, total;
		step = new Statistic.Result(packsInStep.getAndSet(0), bytesInStep.getAndSet(0), now - statsed.getAndSet(now));
		total = new Statistic.Result(packsInTotal.get(), bytesInTotal.get(), new Date().getTime() - begin);
		logger.debug(() -> MessageFormat.format("Statistic: [Step: {0}/objs,{1},{2}], [Total: {3}/objs,{4},{5}], [{6}].", step.packs,
				formatBytes(step.bytes), formatMillis(step.millis), total.packs, formatBytes(total.bytes), formatMillis(total.millis),
				suffixing.get()));
	}

	private static DecimalFormat f = new DecimalFormat("#.##");

	private static long K = 1024;
	private static long M = K * K;
	private static long G = M * K;
	private static long T = G * K;

	String formatBytes(long bytes) {
		double b = bytes;
		if (b > T) return f.format(b / T) + "/TB";
		// +"+" + formatBytes(bytes % T);
		if (b > G) return f.format(b / G) + "/GB";
		// +"+" + formatBytes(bytes % G);
		if (b > M) return f.format(b / M) + "/MB";
		// +"+" + formatBytes(bytes % M);
		if (b > K) return f.format(b / K) + "/KB";
		// +"+" + formatBytes(bytes % K);
		return f.format(b) + "/Bytes";
	}

	private static int SECOND = 1000;
	private static int MINUTE = 60 * SECOND;
	private static int HOUR = 60 * MINUTE;

	String formatMillis(long millis) {
		double ms = millis * 1.0;
		if (ms > HOUR) return f.format(ms / HOUR) + "/h";
		// + "+" + formatMillis(millis % HOUR);
		if (ms > MINUTE) return f.format(ms / MINUTE) + "/m";
		// + "+" + formatMillis(millis % MINUTE);
		if (ms > SECOND) return f.format(millis / SECOND) + "/s";
		// + "+" + formatMillis(millis % SECOND);
		return f.format(ms) + "/ms";
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