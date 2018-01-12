package net.butfly.albacore.expr.fel;

import static net.butfly.albacore.expr.Engine.logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;

import com.greenpineyu.fel.FelEngineImpl;
import com.greenpineyu.fel.common.Null;
import com.greenpineyu.fel.function.CommonFunction;

import net.butfly.albacore.expr.fel.FelFunc.Func;
import net.butfly.albacore.utils.Reflections;

public final class Fels {
	static final Null NULL = new Null();

	static com.greenpineyu.fel.FelEngine scan() {
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

	static boolean isNull(Object arg) {
		return null == arg || Null.class.isAssignableFrom(arg.getClass());
	}
}
