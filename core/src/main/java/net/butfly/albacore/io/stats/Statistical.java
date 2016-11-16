package net.butfly.albacore.io.stats;

import java.io.Serializable;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.Supplier;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.logger.Logger;

public interface Statistical<T extends Statistical<T, V>, V> extends Serializable {
	final static long DEFAULT_STEP = Long.MAX_VALUE;
	static final String LOG_PREFIX = "Queue.Statistic";
	static final Logger slogger = Logger.getLogger(LOG_PREFIX);

	default T trace(long step, Converter<V, Long> sizing, Supplier<Long> current) {
		slogger.info("Staticstic register as [" + LOG_PREFIX + "].[" + this.getClass().getSimpleName() + "]");
		Logger l = Logger.getLogger(LOG_PREFIX + "." + this.getClass().getSimpleName());
		@SuppressWarnings("unchecked")
		T t = (T) this;
		Instances.fetch(() -> new Statistic<T, V>(t, l, step, sizing, current), Statistic.class, this);
		return t;
	}

	default void stats(V v) {
		@SuppressWarnings("unchecked")
		Statistic<T, V> s = Instances.fetch(() -> null, Statistic.class, this);
		if (null != s) s.stats(v);
	}

	default Iterable<V> stats(Iterable<V> vv) {
		@SuppressWarnings("unchecked")
		Statistic<T, V> s = Instances.fetch(() -> null, Statistic.class, this);
		if (null != s) s.stats(vv);
		return vv;
	};
}
