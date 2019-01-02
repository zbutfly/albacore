package net.butfly.albacore.expr.fel;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static net.butfly.albacore.expr.fel.Fels.isNull;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.greenpineyu.fel.function.CommonFunction;

import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.logger.Logger;

public abstract class FelFunc<R> extends CommonFunction {
	final static Logger logger = Logger.getLogger(FelFunc.class);

	@Override
	public final Object call(Object[] args) {
		Object r;
		if (null == args) args = new Object[0];
		if (valid(args.length)) try {
			return null == (r = invoke(args)) ? Fels.NULL : r;
		} catch (Exception e) {
			logger.debug("Expression eval fail", e);
			return Fels.NULL;
		}
		logger.error(getName() + "() by illegal arguments");
		return null;
	}

	protected abstract R invoke(Object... args);

	protected boolean valid(int argl) {
		return true;
	}

	@Override
	public String getName() {
		Func ff = this.getClass().getAnnotation(Func.class);
		if (null != ff && !"".equals(ff.value())) return ff.value();
		String name = this.getClass().getSimpleName();
		if (name.endsWith("Func")) name = name.substring(0, name.length() - 4);
		else if (name.endsWith("Function")) name = name.substring(0, name.length() - 8);
		return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, name);
	}

	@Target(TYPE)
	@Retention(RUNTIME)
	public @interface Func {
		String value() default "";

		String version() default "";
	}

	@Func
	static class IsNullFunc extends FelFunc<Boolean> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1;
		}

		@Override
		public Boolean invoke(Object... args) {
			return isNull(args[0]);
		}
	}

	@Func
	static class EqualFunc extends FelFunc<Boolean> {
		@Override
		protected boolean valid(int argl) {
			return argl == 2;
		}

		@Override
		public Boolean invoke(Object... args) {
			return isNull(args[0]) ? isNull(args[1]) : args[0].equals(args[1]);
		}
	}

	@Func
	static class RandomFunc extends FelFunc<Number> {
		@Override
		public Number invoke(Object... args) {
			if (null == args || 0 == args.length) return Math.random();
			switch (args.length) {
			case 1:
				Number n = (Number) args[0];
				if (n instanceof Integer) return (int) (Math.random() * n.intValue());
				if (n instanceof Long) return (long) (Math.random() * n.longValue());
				if (n instanceof Double) return (double) (Math.random() * n.doubleValue());
				if (n instanceof Float) return (float) (Math.random() * n.floatValue());
				throw new IllegalArgumentException("Not support random generating for " + n.getClass().getName() + " argument.");
			case 2:
				Number n1 = (Number) args[0];
				Number n2 = (Number) args[0];
				if (!n1.getClass().equals(n2.getClass())) throw new IllegalArgumentException("Not support random generating for " //
						+ n1.getClass().getName() + " and " + n2.getClass().getName() + " arguments.");
				Number r = n2.doubleValue() - n1.doubleValue();
				if (n1 instanceof Integer) return (int) (Math.random() * r.intValue() + n1.intValue());
				if (n1 instanceof Long) return (long) (Math.random() * r.longValue() + n1.longValue());
				if (n1 instanceof Double) return (double) (Math.random() * r.doubleValue() + n1.doubleValue());
				if (n1 instanceof Float) return (float) (Math.random() * r.floatValue() + n1.floatValue());
				throw new IllegalArgumentException("Not support random generating for " //
						+ n1.getClass().getName() + " and " + n2.getClass().getName() + " arguments.");
			default:
				throw new IllegalArgumentException("Not support random generating for " + args.length + " arguments.");
			}
		}
	}
}
