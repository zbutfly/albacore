package net.butfly.albacore.expr.fel;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.greenpineyu.fel.function.CommonFunction;

import net.butfly.albacore.utils.CaseFormat;

public abstract class FelFunc extends CommonFunction {
	@Override
	public abstract Object call(Object[] args);

	@Override
	public String getName() {
		Func ff = this.getClass().getAnnotation(Func.class);
		if (null != ff && !"".equals(ff.value())) return ff.value();
		String name = this.getClass().getSimpleName();
		if (name.endsWith("Func")) name = name.substring(0, name.length() - 4);
		else if (name.endsWith("Function")) name = name.substring(0, name.length() - 8);
		return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, name);
	}

	/**
	 * case(value, , re)
	 * 
	 * @author butfly
	 */
	@Func
	private static class CaseFunc extends FelFunc {
		@Override
		public Object call(Object[] args) {
			return args[0];
		}
	}

	@Func
	private static class SubstrFunc extends FelFunc {
		@Override
		public Object call(Object[] args) {
			String srcField = (String) args[0];
			int startIndex = (int) args[1];
			int endIndex = (int) args[2];
			String dstField = srcField.substring(startIndex, endIndex);
			return dstField;
		}
	}

	@Func
	private static class ConcatFunc extends FelFunc {
		@Override
		public Object call(Object[] args) {
			String result = "";
			for (Object a : args)
				result += a.toString();
			return result;
		}
	}

	@Func
	private static class DateToStrFunc extends FelFunc {
		@Override
		public Object call(Object[] args) {
			SimpleDateFormat sdf = new SimpleDateFormat(String.valueOf(args[1]));
			return sdf.format((Date) args[0]);
		}
	}

	@Func
	private static class StrToDateFunc extends FelFunc {
		@Override
		public Object call(Object[] args) {
			SimpleDateFormat sdf = new SimpleDateFormat(String.valueOf(args[1]));
			try {
				return sdf.parse(String.valueOf(args[0]));
			} catch (ParseException e) {
				return null;
			}
		}
	}

	@Target(TYPE)
	@Retention(RUNTIME)
	public @interface Func {
		String value() default "";
	}
}
