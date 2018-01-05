package net.butfly.albacore.expr.fel;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import com.greenpineyu.fel.common.Null;
import com.greenpineyu.fel.function.CommonFunction;

import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.logger.Logger;

public abstract class FelFunc<R> extends CommonFunction {
	final static Logger logger = Logger.getLogger(FelFunc.class);

	@Override
	public final Object call(Object[] args) {
		if (null != args && valid(args.length)) try {
			return invoke(args);
		} catch (Exception e) {
			logger.debug("Expression eval fail", e);
			return new Null();
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
	}
}
