package net.butfly.albacore.io.stats;

import java.io.Serializable;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.Supplier;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.logger.Logger;

public interface Statistical<T extends Statistical<T>> extends Serializable {
	final static long DEFAULT_STEP = Long.MAX_VALUE;
	static final String LOG_PREFIX = "Queue.Statistic";
	static final Logger slogger = Logger.getLogger(LOG_PREFIX);

	default T trace(long step) {
		return trace(step, () -> null);
	}

	default T trace(long step, Converter<Object, Long> sizing) {
		return trace(step, sizing, () -> null);
	}

	default T trace(long step, Supplier<String> detailing) {
		return trace(step, Systems::sizeOf, detailing);
	}

	default T trace(long step, Converter<Object, Long> sizing, Supplier<String> detailing) {
		return trace(null, step, sizing, detailing);
	}

	default T trace(String prefix, long step, Converter<Object, Long> sizing, Supplier<String> detailing) {
		String logname = null == prefix ? LOG_PREFIX : LOG_PREFIX + "." + prefix;
		slogger.info("Staticstic register as [" + logname + "] on step [" + step + "]");
		@SuppressWarnings("unchecked")
		T t = (T) this;
		Instances.fetch(() -> new Statistic(t, logname, step, sizing, detailing), Statistic.class, this);
		return t;
	}

	default Object stats(Object v) {
		Statistic s = Instances.fetch(() -> null, Statistic.class, this);
		if (null != s) s.stats(v);
		return v;
	}

	default void stats(Iterable<?> vv) {
		Statistic s = Instances.fetch(() -> null, Statistic.class, this);
		if (null != s) s.stats(vv);
	}
}
