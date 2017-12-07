package net.butfly.albacore.utils.stats;

import static net.butfly.albacore.utils.logger.Logger.getLogger;
import static net.butfly.albacore.utils.logger.Logger.logex;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.Texts;
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
	final Supplier<String> detailing;
	final Function<Object, Long> sizing;

	<T extends Statistical<T>> Statistic(Statistical<T> owner, String logname, long step, Function<Object, Long> sizing,
			Supplier<String> detailing) {
		Reflections.noneNull("", owner, logname);
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
		Statistic.Result step, total;
		step = new Statistic.Result(packsInStep.getAndSet(0), bytesInStep.getAndSet(0), now - statsed.getAndSet(now));
		total = new Statistic.Result(packsInTotal.get(), bytesInTotal.get(), new Date().getTime() - begin);
		logger.debug(() -> this.traceDetail(step, total));
	}

	private String traceDetail(Statistic.Result step, Statistic.Result total) {
		String ss = null == detailing ? "" : detailing.get();
		ss = null == ss ? "" : ", [" + ss + "]";
		String stepAvg = step.millis > 0 ? Long.toString(step.packs * 1000 / step.millis) : "no_time";
		String totalAvg = total.millis > 0 ? Long.toString(total.packs * 1000 / total.millis) : "no_time";
		return MessageFormat.format("Statistic: [Step: {0}/objs,{1},{2},{7} objs/s], [Total: {3}/objs,{4},{5},{8} objs/s] {6}.", //
				step.packs, Texts.formatKilo(step.bytes, "B"), Texts.formatMillis(step.millis), //
				total.packs, Texts.formatKilo(total.bytes, "B"), Texts.formatMillis(total.millis), //
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

}