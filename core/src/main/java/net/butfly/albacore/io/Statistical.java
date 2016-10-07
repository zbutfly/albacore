package net.butfly.albacore.io;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.logger.Logger;

public interface Statistical<E> extends Serializable {
	static final long SIZE_NULL = -1;

	enum Act {
		INPUT, OUTPUT
	}

	default Converter<E, Long> statsing() {
		return null;
	};

	default E stats(Act act, E e, Supplier<Long> current) {
		if (null == e) return null;
		if (statsing() != null) Instances.fetch(() -> new Statistic(Logger.getLogger("Queue.Statistic." + this.getClass().getSimpleName())),
				Statistic.class, this).stats(Act.INPUT, statsing().apply(e), current);
		return e;
	}

	class Statistic implements Serializable {
		private static final long serialVersionUID = 8773599197517842009L;
		private static final long DEFAULT_STATS_STEP = 1000000;

		private final Logger logger;
		private final AtomicLong packsStep;

		private final long begin;
		private final AtomicLong lastRecord;

		private final AtomicLong packsInStep;
		private final AtomicLong bytesInStep;
		private final AtomicLong packsInTotal;
		private final AtomicLong bytesInTotal;

		private final AtomicLong packsOutStep;
		private final AtomicLong bytesOutStep;
		private final AtomicLong packsOutTotal;
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

		public boolean stats(Act act, long bytes, Supplier<Long> current) {
			long step = -1;
			switch (act) {
			case INPUT:
				packsInTotal.incrementAndGet();
				bytesInTotal.addAndGet(bytes < 0 ? 0 : bytes);
				step = packsInStep.incrementAndGet();
				bytesInStep.addAndGet(bytes < 0 ? 0 : bytes);
			case OUTPUT:
				packsOutTotal.incrementAndGet();
				bytesOutTotal.addAndGet(bytes < 0 ? 0 : bytes);
				step = packsOutStep.incrementAndGet();
				bytesOutStep.addAndGet(bytes < 0 ? 0 : bytes);
			}
			if (step > packsStep.get()) {
				trace(act, current);
				return false;
			} else return true;
		}

		private void trace(Act act, Supplier<Long> current) {
			if (logger.isTraceEnabled()) {
				long now = new Date().getTime();
				Result step, total;
				switch (act) {
				case INPUT:
					step = new Result(packsInStep.getAndSet(0), bytesInStep.getAndSet(0), now - lastRecord.getAndSet(now));
					total = new Result(packsInTotal.get(), bytesInTotal.get(), new Date().getTime() - begin);
					logger.trace("Queue " + act + " Statistic:\n"//
							+ "\tStep: " + step.packs + " records/" + formatBytes(step.bytes) + " in " + formatMillis(step.millis) + "\n"//
							+ "\tTotal: " + total.packs + " records/" + formatBytes(total.bytes) + " in " + formatMillis(total.millis)
							+ "\n"//
							+ "\tcurrent: " + current.get().toString() + "\n"//
					);
				case OUTPUT:
					step = new Result(packsOutStep.getAndSet(0), bytesOutStep.getAndSet(0), now - lastRecord.getAndSet(now));
					total = new Result(packsOutTotal.get(), bytesOutTotal.get(), new Date().getTime() - begin);
					logger.trace("Queue " + act + " Statistic:\n"//
							+ "\tStep: " + step.packs + " records/" + formatBytes(step.bytes) + " in " + formatMillis(step.millis) + "\n"//
							+ "\tTotal: " + total.packs + " records/" + formatBytes(total.bytes) + " in " + formatMillis(total.millis)
							+ "\n"//
							+ "\tcurrent: " + current.get().toString() + "\n"//

					);
				}
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

	}

	class Result {
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
