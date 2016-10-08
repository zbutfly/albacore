package net.butfly.albacore.io.stats;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

import net.butfly.albacore.io.stats.Statistical.Act;
import net.butfly.albacore.support.Values;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.logger.Logger;

class Statistic<V> implements Serializable {
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

	Statistic(Statistical<V> owner, Logger logger, long step) {
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
	}

	protected int sizing(V v) {
		logger.warn("Calculate size of data, time costly!!");
		return Values.sizing(v);
	}

	boolean stats(Act act, V v) {
		if (null == v) return packsInStep.get() > packsStep.get();
		long bytes = sizing(v);
		packsInTotal.incrementAndGet();
		bytesInTotal.addAndGet(bytes < 0 ? 0 : bytes);
		bytesInStep.addAndGet(bytes < 0 ? 0 : bytes);
		return packsInStep.incrementAndGet() > packsStep.get();
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