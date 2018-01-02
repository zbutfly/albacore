package net.butfly.albacore.expr.fel;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import com.greenpineyu.fel.function.CommonFunction;

import net.butfly.albacore.utils.CaseFormat;
import net.butfly.albacore.utils.Texts;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

public abstract class FelFunc<R> extends CommonFunction {
	private final static Logger logger = Logger.getLogger(FelFunc.class);

	@Override
	public final Object call(Object[] args) {
		if (null != args && valid(args.length)) try {
			return invoke(args);
		} catch (Exception e) {
			logger.debug("Expression eval fail", e);
			return null;
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

	/**
	 * uuid()
	 * 
	 * @author butfly
	 */
	@Func
	private static class UuidFunc extends FelFunc<Object> {
		@Override
		public Object invoke(Object... args) {
			return UUID.randomUUID().toString();
		}
	}

	/**
	 * strrev('1234567')
	 * 
	 * @author butfly
	 */
	@Func
	private static class StrrevFunc extends FelFunc<Object> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1;
		}

		@Override
		public Object invoke(Object... args) {
			return null == args[0] ? null : new StringBuilder(args[0].toString()).reverse().toString();
		}
	}

	/**
	 * case(value, case1, result1, case2, result2, ... [default])
	 * 
	 * @author butfly
	 */
	@Func
	private static class CaseFunc extends FelFunc<Object> {
		@Override
		protected boolean valid(int argl) {
			return argl > 0;
		}

		@Override
		public Object invoke(Object... args) {
			Object v0 = args[0];
			int i = 1;
			while (i < args.length) {
				Object case1 = args[i++];
				if (i < args.length) { // pair case/result, test match and process
					Object value1 = args[i++];
					if (match(v0, case1)) return value1;
				} else return case1;// odd args, with default value, match default value
			}
			return null; // no matchs and no default
		}

		private boolean match(Object v, Object cas) {
			if (null == v && null == cas) return true;
			if (null != v && null != cas) return v.equals(cas);
			return false;
		}
	}

	/**
	 * match(value, regularExpression)
	 * 
	 * @author butfly
	 */
	@Func
	private static class MatchFunc extends FelFunc<Boolean> {
		private static final Map<String, Pattern> patterns = Maps.of();

		@Override
		protected boolean valid(int argl) {
			return argl == 2;
		}

		@Override
		public Boolean invoke(Object... args) {
			return patterns.computeIfAbsent((String) args[1], Pattern::compile).matcher((String) args[0]).find();
		}
	}

	@Func
	private static class SubstrFunc extends FelFunc<String> {
		@Override
		protected boolean valid(int argl) {
			return argl == 3;
		}

		@Override
		public String invoke(Object... args) {
			return ((String) args[0]).substring((int) args[1], (int) args[2]);
		}
	}

	@Deprecated
	@Func
	private static class ConcatFunc extends FelFunc<String> {
		@Override
		public String invoke(Object... args) {
			String result = "";
			for (Object a : args)
				result += a.toString();
			return result;
		}
	}

	@Func
	private static class DateToStrFunc extends FelFunc<String> {
		@Override
		protected boolean valid(int argl) {
			return argl == 2;
		}

		@Override
		public String invoke(Object... args) {
			return Texts.formatDate((String) args[1], (Date) args[0]);
		}
	}

	@Func
	private static class StrToDateFunc extends FelFunc<Date> {
		@Override
		protected boolean valid(int argl) {
			return argl == 2;
		}

		@Override
		public Date invoke(Object... args) {
			try {
				return Texts.parseDate(String.valueOf(args[1]), String.valueOf(args[0]));
			} catch (ParseException e) {
				logger.debug("Expression eval for date parsing fail", e);
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
