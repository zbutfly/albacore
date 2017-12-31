package net.butfly.albacore.expr;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import net.butfly.albacore.Albacore;
import net.butfly.albacore.expr.fel.FelEngine;
import net.butfly.albacore.utils.Configs;
import net.butfly.albacore.utils.logger.Logger;

public interface Engine {
	static final Logger logger = Logger.getLogger(Engine.class);

	<T> T exec(String expr, Map<String, Object> context);

	static <T> T eval(String expr, Map<String, Object> context) {
		return Default.def.exec(expr, context);
	}

	static class Default {
		private static final Engine def = scan();

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
