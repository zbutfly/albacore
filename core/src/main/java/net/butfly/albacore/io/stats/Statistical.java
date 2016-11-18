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
		return trace("", step, sizing, current);
	}

	default T trace(String prefix, long step, Converter<V, Long> sizing, Supplier<Long> current) {
		String logname = LOG_PREFIX + "." + prefix;
		slogger.info("Staticstic register as [" + logname + "]");
		Logger l = Logger.getLogger(logname);
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

	default <VV extends Iterable<V>> VV stats(VV vv) {
		@SuppressWarnings("unchecked")
		Statistic<T, V> s = Instances.fetch(() -> null, Statistic.class, this);
		if (null != s) s.stats(vv);
		return vv;
	};
}
