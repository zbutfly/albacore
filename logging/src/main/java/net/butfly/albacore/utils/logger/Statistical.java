package net.butfly.albacore.utils.logger;

import java.util.function.Function;
import java.util.function.Supplier;

import net.butfly.albacore.utils.JVM;
import net.butfly.albacore.utils.Syss;

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
		return trace(step, Syss::sizeOf, detailing);
	}

	default T trace(long step, Function<Object, Long> sizing, Supplier<String> detailing) {
		return trace(null, step, sizing, detailing);
	}

	@SuppressWarnings("unchecked")
	default T trace(String prefix, long step, Function<Object, Long> sizing, Supplier<String> detailing) {
		Statistic.STATISTICS.computeIfAbsent(this, cal -> {
			String logname = JVM.current().mainClass.getName() + "." + (null == prefix ? getClass().getSimpleName() : prefix);
			slogger.info(() -> "Staticstic register as [" + logname + "] on step [" + step + "]");
			return new Statistic(cal, logname, step, sizing, detailing);
		});
		return (T) this;
	}

	default <V> V stats(V v) {
		Statistic s = Statistic.STATISTICS.get(this);
		if (null != s) s.stats(v);
		return v;
	}

	default void traceForce(String curr) {
		Statistic s = Statistic.STATISTICS.get(this);
		if (null != s) s.trace(curr);
	}

	default <V> void stats(Iterable<V> vv) {
		Statistic s = Statistic.STATISTICS.get(this);
		if (null != s) for (V v : vv)
			Logger.logex.execute(() -> s.stats(v));
	}
}
