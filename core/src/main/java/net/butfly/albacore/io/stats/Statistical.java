package net.butfly.albacore.io.stats;

import java.io.Serializable;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.Supplier;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.logger.Logger;

public interface Statistical<T extends Statistical<T>> extends Serializable {
	final static long DEFAULT_STEP = Long.MAX_VALUE;
	static final String LOG_PREFIX = "Queue.Statistic";
	static final Logger slogger = Logger.getLogger(LOG_PREFIX);

	default T trace(long step, Converter<Object, Long> sizing, Supplier<String> suffixing) {
		return trace("", step, sizing, suffixing);
	}

	default T trace(String prefix, long step, Converter<Object, Long> sizing, Supplier<String> suffixing) {
		String logname = LOG_PREFIX + "." + prefix;
		slogger.info("Staticstic register as [" + logname + "] on step [" + step + "]");
		Logger l = Logger.getLogger(logname);
		@SuppressWarnings("unchecked")
		T t = (T) this;
		Instances.fetch(() -> new Statistic(t, l, step, sizing, suffixing), Statistic.class, this);
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
