package net.butfly.albacore.io.stats;

import java.util.stream.Stream;

import net.butfly.albacore.io.Streams;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.Supplier;
import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.logger.Logger;

public interface Statistical<T extends Statistical<T>> {
	final static long DEFAULT_STEP = Long.MAX_VALUE;
	static final String LOG_PREFIX = "A.Statistic";
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

	default <V> void stats(V v) {
		Statistic s = Instances.fetch(() -> null, Statistic.class, this);
		if (null != s) s.stats(v);
	}

	default <V> void stats(Iterable<V> vv) {
		Statistic s = Instances.fetch(() -> null, Statistic.class, this);
		if (null != s) Streams.of(vv).forEach(v -> s.stats(v));
	}

	default <V> Stream<V> stats(Stream<V> vv) {
		Statistic s = Instances.fetch(() -> null, Statistic.class, this);
		return null != s ? vv.peek(v -> {
			if (null != v) s.stats(v);
		}) : vv;
	}
}
