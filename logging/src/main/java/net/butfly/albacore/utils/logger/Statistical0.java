package net.butfly.albacore.utils.logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

import net.butfly.albacore.utils.JVM;
import net.butfly.albacore.utils.Syss;
import static net.butfly.albacore.utils.logger.LogExec.tryExec;

@Deprecated
public interface Statistical0<T extends Statistical0<T>> {
	final static long DEFAULT_STEP = Long.MAX_VALUE;
	static final Logger slogger = Logger.getLogger(Statistic1.class);

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
		Statistic1.STATISTICS.computeIfAbsent(this, cal -> {
			String logname = JVM.current().mainClass.getName() + "." + (null == prefix ? getClass().getSimpleName() : prefix);
			slogger.info(() -> "Staticstic register as [" + logname + "] on step [" + step + "]");
			return new Statistic1(logname, step, sizing, detailing);
		});
		return (T) this;
	}

	default <V> V stats(V v) {
		Statistic1 s = Statistic1.STATISTICS.get(this);
		if (null != s) s.stats(v);
		return v;
	}

	default void traceForce(String curr) {
		Statistic1 s = Statistic1.STATISTICS.get(this);
		if (null != s) s.trace(curr);
	}

	default <V> void stats(Iterable<V> vv) {
		Statistic1 s = Statistic1.STATISTICS.get(this);
		if (null != s) for (V v : vv)
			tryExec(() -> s.stats(v));
	}

	class Statistic1 extends Statistic {
		static Map<Statistical0<?>, Statistic1> STATISTICS = new ConcurrentHashMap<>();

		Statistic1(String logname, long step, Function<Object, Long> sizing, Supplier<String> detailing) {
			super(logname);
			step(step).sizing(sizing).detailing(detailing);
		}

		void trace(String info) {
			logger.debug(info);
		}
	}
}
