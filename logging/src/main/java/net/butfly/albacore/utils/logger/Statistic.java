package net.butfly.albacore.utils.logger;

import static net.butfly.albacore.utils.logger.Logger.getLogger;
import static net.butfly.albacore.utils.logger.Logger.logex;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

import net.butfly.albacore.utils.logger.Logger;
import static net.butfly.albacore.Albacore.Props.PROP_PARALLEL_POOL_SIZE_OBJECT;

class Statistic implements Serializable {
	private static final long serialVersionUID = -1;
	static Map<Statistical<?>, Statistic> STATISTICS = new ConcurrentHashMap<>();
	final ReentrantLock lock;

	final Logger logger;
	final AtomicLong packsStep;

	final long begin;
	final AtomicLong statsed;

	final AtomicLong packsInStep;
	final AtomicLong bytesInStep;
	final AtomicLong packsInTotal;
	final AtomicLong bytesInTotal;
	final Supplier<String> detailing;
	final Function<Object, Long> sizing;

	<T extends Statistical<T>> Statistic(Statistical<T> owner, String logname, long step, Function<Object, Long> sizing,
			Supplier<String> detailing) {
		lock = new ReentrantLock();
		logger = getLogger(logname);
		packsStep = new AtomicLong(step - 1);
		packsInStep = new AtomicLong(0);
		bytesInStep = new AtomicLong(0);
		packsInTotal = new AtomicLong(0);
		bytesInTotal = new AtomicLong(0);
		begin = new Date().getTime();
		statsed = new AtomicLong(begin);
		this.sizing = sizing;
		this.detailing = detailing;
	}

	void stats(long bytes) {
		packsInTotal.incrementAndGet();
		bytesInTotal.addAndGet(bytes < 0 ? 0 : bytes);
		bytesInStep.addAndGet(bytes < 0 ? 0 : bytes);
		if (packsInStep.incrementAndGet() > packsStep.get() && logger.isInfoEnabled() && lock.tryLock()) try {
			trace();
		} finally {
			lock.unlock();
		}
	}

	<T> T stats(T v) {
		try {
			logex.submit(() -> {
				long size;
				if (sizing == null || v == null) size = 0;
				else try {
					Long s = sizing.apply(v);
					size = null == s ? 0 : s.longValue();
				} catch (Throwable t) {
					size = 0;
				}
				stats(size);
			});
		} catch (RejectedExecutionException e) {}
		return v;
	}

	void trace() {
		long now = new Date().getTime();
		Result step, total;
		step = new Result(packsInStep.getAndSet(0), bytesInStep.getAndSet(0), now - statsed.getAndSet(now));
		total = new Result(packsInTotal.get(), bytesInTotal.get(), new Date().getTime() - begin);
		logger.debug(() -> this.traceDetail(step, total));
	}

	private String traceDetail(Result step, Result total) {
		String ss = null;
		if (null == detailing) ss = detailing.get();
		ss = null == ss ? "" : ", [" + ss + "]";
		String stepAvg = step.millis > 0 ? Long.toString(step.packs * 1000 / step.millis) : "no_time";
		String totalAvg = total.millis > 0 ? Long.toString(total.packs * 1000 / total.millis) : "no_time";
		return MessageFormat.format("Statistic: [Step: {0}/objs,{1},{2},{7} objs/s], [Total: {3}/objs,{4},{5},{8} objs/s] {6}.", //
				step.packs, formatKilo(step.bytes, "B"), formatMillis(step.millis), //
				total.packs, formatKilo(total.bytes, "B"), formatMillis(total.millis), //
				ss, stepAvg, totalAvg);
	}

	void trace(String info) {
		logger.debug(info);
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

	static String formatKilo(double d, String unit) {
		DecimalFormat f = DEC_FORMATS.poll();
		if (null == f) f = new DecimalFormat("#.##");
		try {
			// double d = n;
			if (d > T) return f.format(d / T) + "T" + unit;
			// +"+" + formatBytes(bytes % T);
			if (d > G) return f.format(d / G) + "G" + unit;
			// +"+" + formatBytes(bytes % G);
			if (d > M) return f.format(d / M) + "M" + unit;
			// +"+" + formatBytes(bytes % M);
			if (d > K) return f.format(d / K) + "K" + unit;
			// +"+" + formatBytes(bytes % K);
			return f.format(d) + unit;
		} finally {
			DEC_FORMATS.offer(f);
		}
	}

	private static final int POOL_SIZE = Integer.parseInt(System.getProperty(PROP_PARALLEL_POOL_SIZE_OBJECT, //
			Integer.toString(Runtime.getRuntime().availableProcessors() - 1)));
	private static LinkedBlockingQueue<DecimalFormat> DEC_FORMATS = new LinkedBlockingQueue<>(POOL_SIZE);

	private static long K = 1024;
	private static long M = K * K;
	private static long G = M * K;
	private static long T = G * K;
	private static int SECOND = 1000;
	private static int MINUTE = 60 * SECOND;
	private static int HOUR = 60 * MINUTE;

	static String formatMillis(double millis) {
		DecimalFormat f = DEC_FORMATS.poll();
		if (null == f) f = new DecimalFormat("#.##");
		try {
			if (millis > HOUR) return f.format(millis / HOUR) + " Hours";
			// + "+" + formatMillis(millis % HOUR);
			if (millis > MINUTE) return f.format(millis / MINUTE) + " Minutes";
			// + "+" + formatMillis(millis % MINUTE);
			if (millis > SECOND) return f.format(millis / SECOND) + " Secs";
			// + "+" + formatMillis(millis % SECOND);
			return f.format(millis) + " MS";
		} finally {
			DEC_FORMATS.offer(f);
		}
	}
}