package net.butfly.albacore.io.stats;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.logger.Logger;

public interface Statistical<E> extends Serializable {
	static final long SIZE_NULL = -1;
	static final Logger slogger = Logger.getLogger(Statistical.class);
	static final String LOG_PREFIX = "Queue.Statistic.";

	enum Act {
		INPUT, OUTPUT
	}

	default void statsRegister(Converter<E, Long> sizing, Act... act) {
		for (Act a : act) {
			slogger.info("Staticstic register act [" + a.name() + "] as [" + LOG_PREFIX + "].[" + this.getClass().getSimpleName() + "]");
			Instances.fetch(() -> new Statistic<E>(this, Logger.getLogger(LOG_PREFIX + this.getClass().getSimpleName()), sizing),
					new TypeToken<Statistic<E>>() {
						private static final long serialVersionUID = -7550073495369710703L;
					}, this, a);
		}
	}

	default Statistic<E> stats(Act act) {
		return Instances.fetch(new TypeToken<Statistic<E>>() {
			private static final long serialVersionUID = 5959776054841470308L;
		}, this, act);
	}

	default E statsRecord(Act act, E e, Supplier<Long> current) {
		if (null == e) return null;
		@SuppressWarnings("unchecked")
		Statistic<E> s = Instances.fetch(Statistic.class, this, e.getClass());
		if (null == s) return e;
		return s.stats(act, e, current);
	}

	final static class Statistic<E> implements Serializable {
		private static final long serialVersionUID = -1;
		private static final long DEFAULT_STATS_STEP = 1000000;

		private final Logger logger;
		private final AtomicLong packsStep;

		private final long begin;
		private final AtomicLong lastRecord;

		private final AtomicLong packsInStep;
		private final AtomicLong bytesInStep;
		private final AtomicLong packsInTotal;
		private final AtomicLong bytesInTotal;
		private final Converter<E, Long> sizing;

		public Statistic(Statistical<E> owner, Logger logger, Converter<E, Long> sizing) {
			this.logger = logger;
			this.sizing = sizing;
			this.packsStep = new AtomicLong(DEFAULT_STATS_STEP);
			packsInStep = new AtomicLong(0);
			bytesInStep = new AtomicLong(0);
			packsInTotal = new AtomicLong(0);
			bytesInTotal = new AtomicLong(0);
			begin = new Date().getTime();
			lastRecord = new AtomicLong(begin);
		}

		private E stats(Act act, E e, Supplier<Long> current) {
			if (null == e) return null;
			if (sizing == null) return e;
			long bytes = sizing.apply(e);
			long step = -1;
			packsInTotal.incrementAndGet();
			bytesInTotal.addAndGet(bytes < 0 ? 0 : bytes);
			step = packsInStep.incrementAndGet();
			bytesInStep.addAndGet(bytes < 0 ? 0 : bytes);
			if (step > packsStep.get()) trace(act, current);
			return e;
		}

		private void trace(Act act, Supplier<Long> current) {
			if (logger.isTraceEnabled()) {
				long now = new Date().getTime();
				Result step, total;
				step = new Result(packsInStep.getAndSet(0), bytesInStep.getAndSet(0), now - lastRecord.getAndSet(now));
				total = new Result(packsInTotal.get(), bytesInTotal.get(), new Date().getTime() - begin);
				logger.trace("Queue " + act + " Statistic:\n"//
						+ "\tStep: " + step.packs + " records/" + formatBytes(step.bytes) + " in " + formatMillis(step.millis) + "\n"//
						+ "\tTotal: " + total.packs + " records/" + formatBytes(total.bytes) + " in " + formatMillis(total.millis) + "\n"//
						+ "\tcurrent: " + current.get().toString() + "\n"//
				);
			}
		}

		private static DecimalFormat f = new DecimalFormat("#.##");

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

		private static class Result {
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
}
