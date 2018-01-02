package net.butfly.albacore.expr.fel;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Map.Entry;

import com.greenpineyu.fel.Expression;
import com.greenpineyu.fel.FelEngineImpl;
import com.greenpineyu.fel.context.ArrayCtxImpl;
import com.greenpineyu.fel.context.FelContext;
import com.greenpineyu.fel.exception.ParseException;
import com.greenpineyu.fel.function.CommonFunction;

import net.butfly.albacore.expr.Engine;
import net.butfly.albacore.expr.fel.FelFunc.Func;
import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.collection.Maps;

public class FelEngine implements Engine {
	private final static com.greenpineyu.fel.FelEngine engine = scan();

	private static com.greenpineyu.fel.FelEngine scan() {
		FelEngineImpl eng;
		try {
			eng = new FelEngineImpl();
		} catch (Exception e) {
			logger.error("Some fail?", e);
			eng = null;
		}
		for (Class<?> c : Reflections.getClassesAnnotatedWith(Func.class, ""))
			if (CommonFunction.class.isAssignableFrom(c)) {
				if (!Modifier.isStatic(c.getModifiers())) //
					logger.error("FelFunc found func class [" + c.getName() + "] but not static, ignore.");
				else try {
					Constructor<?> cc = c.getDeclaredConstructor();
					if (cc.trySetAccessible()) {
						CommonFunction f = (CommonFunction) cc.newInstance();
						logger.debug("FelEngine function scaned: " + f.getClass().getSimpleName() + "(" + f.getName() + ")");
						if (c.isAnnotationPresent(Deprecated.class)) //
							logger.warn("FelEngine function scaned: " + f.getClass().getSimpleName() + "(" + f.getName()
									+ ") but marked as Deprecated, don't use it.");
						eng.addFun(f);
					}
				} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
						| SecurityException | NoSuchMethodException e) {
					logger.error("FelFunc found func class [" + c.getName() + "] but instantial fail", e);
				}
			} else logger.error("FelFunc found func class [" + c.getName() + "] annotated by @Func is not a FelFunc");
		return eng;
	}

	private final static Map<String, Expression> exprs = Maps.of();

	@SuppressWarnings("unchecked")
	@Override
	public <T> T exec(String felExpr, Map<String, Object> context) {
		FelContext ctx = new ArrayCtxImpl();
		if (null != context && !context.isEmpty()) for (Entry<String, Object> e : context.entrySet())
			ctx.set(e.getKey(), e.getValue());
		Expression ex = exprs.computeIfAbsent(felExpr, expr -> {
			try {
				return engine.compile(felExpr, ctx);
			} catch (ParseException e) {
				logger.error("Expression parsing fail", e);
				return null;
			}
		});
		return null == ex ? null : (T) ex.eval(ctx);
	}
}
