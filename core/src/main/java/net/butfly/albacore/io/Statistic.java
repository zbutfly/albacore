package net.butfly.albacore.io;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.utils.logger.Logger;

class Statistic implements Serializable {
	private static final long serialVersionUID = 8773599197517842009L;
	private static final long DEFAULT_STATS_STEP = 1000000;
	private final Logger logger;
	private final AtomicLong packsStep;

	private final long begin;
	private final AtomicLong lastRecord;
	private final AtomicLong packsInStep;
	private final AtomicLong packsOutStep;
	private final AtomicLong bytesInStep;
	private final AtomicLong bytesOutStep;

	private final AtomicLong packsInTotal;
	private final AtomicLong packsOutTotal;
	private final AtomicLong bytesInTotal;
	private final AtomicLong bytesOutTotal;

	public Statistic(Logger logger) {
		this.logger = logger;
		this.packsStep = new AtomicLong(DEFAULT_STATS_STEP);
		packsInStep = new AtomicLong(0);
		packsOutStep = new AtomicLong(0);
		bytesInStep = new AtomicLong(0);
		bytesOutStep = new AtomicLong(0);
		packsInTotal = new AtomicLong(0);
		packsOutTotal = new AtomicLong(0);
		bytesInTotal = new AtomicLong(0);
		bytesOutTotal = new AtomicLong(0);
		begin = new Date().getTime();
		lastRecord = new AtomicLong(begin);
	}

	public void step(long step) {
		this.packsStep.set(step);
	}

	public void in(long bytes) {
		packsInTotal.incrementAndGet();
		bytesInTotal.addAndGet(bytes < 0 ? 0 : bytes);
		long step = packsInStep.incrementAndGet();
		bytesInStep.addAndGet(bytes < 0 ? 0 : bytes);
		if (step > packsStep.get()) trace("input", inStep(), inTotal());
	}

	public void out(long bytes) {
		packsOutTotal.incrementAndGet();
		bytesOutTotal.addAndGet(bytes < 0 ? 0 : bytes);
		long step = packsInStep.incrementAndGet();
		bytesInStep.addAndGet(bytes < 0 ? 0 : bytes);
		if (step > packsStep.get()) trace("output", outStep(), outTotal());
	}

	private Stats inStep() {
		long now = new Date().getTime();
		return new Stats(packsInStep.getAndSet(0), bytesInStep.getAndSet(0), now - lastRecord.getAndSet(now));
	}

	private Stats inTotal() {
		return new Stats(packsInTotal.get(), bytesInTotal.get(), new Date().getTime() - begin);
	}

	private Stats outStep() {
		long now = new Date().getTime();
		return new Stats(packsOutStep.getAndSet(0), bytesOutStep.getAndSet(0), now - lastRecord.getAndSet(now));
	}

	private Stats outTotal() {
		return new Stats(packsOutTotal.get(), bytesOutTotal.get(), new Date().getTime() - begin);
	}

	private static DecimalFormat f = new DecimalFormat("#.##");

	private void trace(String act, Stats step, Stats total) {
		if (logger.isTraceEnabled()) logger.trace("Queue " + act + " Statistic:\n"//
				+ "\tStep: " + step.packs + " records/" + formatBytes(step.bytes) + " in " + formatMillis(step.millis) + "\n"//
				+ "\tTotal: " + total.packs + " records/" + formatBytes(total.bytes) + " in " + formatMillis(total.millis) + "\n"//
		);
	}

	private static double K = 1024;
	private static double M = K * K;
	private static double G = M * K;
	private static double T = G * K;

	private String formatBytes(long bytes) {
		String bb;
		if (bytes > T * 0.8) bb = f.format(bytes / T) + "TB";
		else if (bytes > G * 0.8) bb = f.format(bytes / G) + "GB";
		else if (bytes > M * 0.8) bb = f.format(bytes / M) + "MB";
		else if (bytes > K * 0.8) bb = f.format(bytes / K) + "KB";
		else bb = f.format(bytes) + "Byte";
		return bb;
	}

	private String formatMillis(long millis) {
		return f.format(millis / 1000.0) + "ms";
	}

	private static class Stats {
		public final long packs;
		public final long bytes;
		public final long millis;

		public Stats(long packs, long bytes, long millis) {
			super();
			this.packs = packs;
			this.bytes = bytes;
			this.millis = millis;
		}
	}

}
