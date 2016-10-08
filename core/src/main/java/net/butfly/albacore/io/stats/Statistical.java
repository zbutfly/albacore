package net.butfly.albacore.io.stats;

import java.io.Serializable;
import java.util.Date;
import java.util.function.Supplier;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.logger.Logger;

public interface Statistical<V> extends Serializable {
	final static long DEFAULT_STEP = Long.MAX_VALUE;
	static final String LOG_PREFIX = "Queue.Statistic";
	static final Logger slogger = Logger.getLogger(LOG_PREFIX);

	enum Act {
		INPUT, OUTPUT
	}

	default TypeToken<Statistic<V>> token() {
		return new TypeToken<Statistic<V>>() {
			private static final long serialVersionUID = -7550073495369710703L;
		};
	}

	default void statsRegister(Converter<V, Integer> sizing, long step, Act... act) {
		for (Act a : act) {
			slogger.info("Staticstic register act [" + a.name() + "] as [" + LOG_PREFIX + "].[" + this.getClass().getSimpleName() + "]");
			Logger l = Logger.getLogger(LOG_PREFIX + "." + this.getClass().getSimpleName());
			Instances.fetch(() -> new Statistic<V>(this, l, step) {
				private static final long serialVersionUID = -3113573795619829610L;

				@Override
				protected int sizing(V v) {
					return sizing.apply(v);
				}
			}, token(), this, a);
		}
	}

	default void statsRegister(long step, Act... act) {
		for (Act a : act) {
			slogger.info("Staticstic register act [" + a.name() + "] as [" + LOG_PREFIX + "].[" + this.getClass().getSimpleName() + "]");
			Logger l = Logger.getLogger(LOG_PREFIX + "." + this.getClass().getSimpleName());
			Instances.fetch(() -> new Statistic<V>(this, l, step), token(), this, a);
		}
	}

	static <E> void statsTrace(Statistic<E> stats, Act act, Supplier<Long> current) {
		if (stats.logger.isTraceEnabled()) {
			long now = new Date().getTime();
			Statistic.Result step, total;
			step = new Statistic.Result(stats.packsInStep.getAndSet(0), stats.bytesInStep.getAndSet(0), now - stats.statsed.getAndSet(now));
			total = new Statistic.Result(stats.packsInTotal.get(), stats.bytesInTotal.get(), new Date().getTime() - stats.begin);
			stats.logger.trace("Queue " + act + " Statistic:\n"//
					+ "\tStep: " + step.packs + " records/" + stats.formatBytes(step.bytes) + " in " + stats.formatMillis(step.millis)
					+ "\n"//
					+ "\tTotal: " + total.packs + " records/" + stats.formatBytes(total.bytes) + " in " + stats.formatMillis(total.millis)
					+ "\n"//
					+ "\tcurrent: " + current.get().toString() + "\n"//
			);
		}
	}

	default V stats(Act act, V e, Supplier<Long> current) {
		Statistic<V> s = Instances.fetch(token(), this, e.getClass());
		if (null != s && s.stats(act, e)) statsTrace(s, act, current);
		return e;
	}

	default V stats(Act act, Supplier<Long> current) {
		return stats(act, null, current);
	}
}
