package net.butfly.albacore.expr;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import net.butfly.albacore.Albacore;
import net.butfly.albacore.expr.fel.FelEngine;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.logger.Logger;

public interface Engine {
	static final Logger logger = Logger.getLogger(Engine.class);

	<T> T exec(String expr, Map<String, Object> context);

	static <T> T eval(String expr) {
		return eval(expr, null);
	}

	static <T> T eval(String expr, Map<String, Object> context) {
		long now = System.currentTimeMillis();
		try {
			return Default.def.exec(expr, context);
		} catch (Exception e) {
			String err = "Expression [" + expr + "] eval fail on context";
			if (null != context && !context.isEmpty()) err += ": \n\t" + context.toString();
			else err += " with empty context.";
			logger.error(err, e);
			return null;
		} finally {
			if (logger.isDebugEnabled() && Default.STATS_STEP > 0) {
				long spent = Default.execSpent.addAndGet(System.currentTimeMillis() - now);
				long count = Default.execCount.incrementAndGet();
				logger.debug(() -> "Express [" + Default.STATS_CLASS + "] exec [" + count + "] times, "//
						+ "average [" + spent * 1.0 / count + " ms].");
			}
		}
	}

	static class Default {
		static final Engine def = scan();
		private static final int STATS_STEP = Integer.parseInt(Configs.gets("albacore.expr.stats.step", "0"));
		private static final String STATS_CLASS = Default.def.getClass().getSimpleName();
		private static final AtomicLong execCount = new AtomicLong();
		private static final AtomicLong execSpent = new AtomicLong();

		private static Engine scan() {
			String cname = Configs.of().gets(Albacore.Props.PROP_EXPR_ENGINE_CLASS, FelEngine.class.getName());
			logger.info("Expression engine [" + cname + "] loaded.");
			try {
				return (Engine) Class.forName(cname).getConstructor().newInstance();
			} catch (InvocationTargetException e) {
				throw new RuntimeException(e.getTargetException());
			} catch (InstantiationException | IllegalAccessException | NoSuchMethodException | ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
	}
}
