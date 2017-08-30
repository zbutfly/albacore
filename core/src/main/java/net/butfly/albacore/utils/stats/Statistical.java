package net.butfly.albacore.utils.stats;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import net.butfly.albacore.utils.Instances;
import net.butfly.albacore.utils.Systems;
import net.butfly.albacore.utils.collection.Streams;
import net.butfly.albacore.utils.logger.Logger;

public interface Statistical<T extends Statistical<T>> {
	final static long DEFAULT_STEP = Long.MAX_VALUE;
	static final Logger slogger = Logger.getLogger(Statistic.class);

	default T trace(long step) {
		return trace(step, () -> null);
	}

	default T trace(long step, Function<Object, Long> sizing) {
		return trace(step, sizing, () -> null);
	}

	default T trace(long step, Supplier<String> detailing) {
		return trace(step, Systems::sizeOf, detailing);
	}

	default T trace(long step, Function<Object, Long> sizing, Supplier<String> detailing) {
		return trace(null, step, sizing, detailing);
	}

	default T trace(String prefix, long step, Function<Object, Long> sizing, Supplier<String> detailing) {
		String logname = Systems.getMainClass().getName() + "." + (null == prefix ? getClass().getSimpleName() : prefix);
		slogger.info(() -> "Staticstic register as [" + logname + "] on step [" + step + "]");
		@SuppressWarnings("unchecked")
		T t = (T) this;
		Instances.fetch(() -> new Statistic(t, logname, step, sizing, detailing), Statistic.class, this);
		return t;
	}

	default <V> void stats(V v) {
		Statistic s = Instances.fetch(() -> null, Statistic.class, this);
		if (null != s) s.stats(v);
	}

	default void traceForce(String curr) {
		Statistic s = Instances.fetch(() -> null, Statistic.class, this);
		if (null != s) s.trace(curr);
	}

	default <V> void stats(Iterable<V> vv) {
		Statistic s = Instances.fetch(() -> null, Statistic.class, this);
		if (null != s) Streams.of(vv).forEach(v -> s.stats(v));
	}

	default <V> Stream<V> stats(Stream<V> vv) {
		Statistic s = Instances.fetch(() -> null, Statistic.class, this);
		return null != s ? vv.peek(s::stats) : vv;
	}
}
