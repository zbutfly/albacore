package net.butfly.albacore.expr.fel;

import static net.butfly.albacore.expr.fel.Fels.isNull;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import com.greenpineyu.fel.common.Null;

import net.butfly.albacore.expr.fel.FelFunc.Func;
import net.butfly.albacore.utils.collection.Maps;

public interface FuncForStr {
	/**
	 * strlen(str)：长度
	 * 
	 * @author butfly
	 */
	@Func
	class StrlenFunc extends FelFunc<Integer> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1;
		}

		@Override
		public Integer invoke(Object... args) {
			return isNull(args[0]) ? 0 : args[0].toString().length();
		}
	}

	/**
	 * uuid()
	 * 
	 * @author butfly
	 */
	@Func
	class UuidFunc extends FelFunc<Object> {
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
	class StrrevFunc extends FelFunc<Object> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1;
		}

		@Override
		public Object invoke(Object... args) {
			return isNull(args[0]) ? new Null() : new StringBuilder(args[0].toString()).reverse().toString();
		}
	}

	/**
	 * case(value, case1, result1, case2, result2, ... [default])
	 * 
	 * @author butfly
	 */
	@Func
	class CaseFunc extends FelFunc<Object> {
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
			return new Null(); // no matchs and no default
		}

		private boolean match(Object v, Object cas) {
			boolean nv = isNull(v), nc = isNull(cas);
			if (nv && nc) return true;
			if (!nv && !nc) return v.equals(cas);
			return false;
		}
	}

	/**
	 * match(value, regularExpression)
	 * 
	 * @author butfly
	 */
	@Func
	class MatchFunc extends FelFunc<Boolean> {
		final Map<String, Pattern> patterns = Maps.of();

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
	class SubstrFunc extends FelFunc<String> {
		@Override
		protected boolean valid(int argl) {
			return argl == 3;
		}

		@Override
		public String invoke(Object... args) {
			return ((String) args[0]).substring((int) args[1], (int) args[2]);
		}
	}

	/**
	 * strpadl(str, len, char)：左填充字符c直到结果字符串长度为l
	 * 
	 * @author butfly
	 */
	@Func
	class StrpadlFunc extends FelFunc<String> {
		@Override
		protected boolean valid(int argl) {
			return argl >= 1 || argl <= 3;
		}

		@Override
		public String invoke(Object... args) {
			String s = isNull(args[0]) ? "" : args[0].toString();
			int l = isNull(args[1]) ? 0 : ((Number) args[1]).intValue();
			if (l <= s.length()) return s;
			char c = 3 == args.length ? StrfilFunc.checkChar(args[2]) : ' ';
			return StrfilFunc.fill(c, l - s.length()) + s;
		}
	}

	/**
	 * strpadr(str, len, char)：右填充字符c直到结果字符串长度为l
	 * 
	 * @author butfly
	 */
	@Func
	class StrpadrFunc extends FelFunc<String> {
		@Override
		protected boolean valid(int argl) {
			return argl >= 1 || argl <= 3;
		}

		@Override
		public String invoke(Object... args) {
			String s = isNull(args[0]) ? "" : args[0].toString();
			int l = isNull(args[1]) ? 0 : ((Number) args[1]).intValue();
			if (l <= s.length()) return s;
			char c = 3 == args.length ? StrfilFunc.checkChar(args[2]) : ' ';
			return s + StrfilFunc.fill(c, l - s.length());
		}
	}

	/**
	 * strfil(n, char)：重复字符n次创建字符串
	 * 
	 * @author butfly
	 */
	@Func
	class StrfilFunc extends FelFunc<String> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1 || argl == 2;
		}

		@Override
		public String invoke(Object... args) {
			int n = ((Number) args[0]).intValue();
			if (0 == n) return "";
			return fill(1 == args.length ? ' ' : checkChar(args[1]), n);
		}

		static char checkChar(Object object) {
			if (isNull(object)) return ' ';
			Class<?> cl = object.getClass();
			if (Character.class.isAssignableFrom(cl)) return ((Character) object).charValue();
			else if (char.class.isAssignableFrom(cl)) return (char) object;
			else if (Number.class.isAssignableFrom(cl)) return (char) ((Number) object).intValue();
			else if (CharSequence.class.isAssignableFrom(cl)) {
				CharSequence cs = ((CharSequence) object);
				return cs.length() == 0 ? ' ' : ((CharSequence) object).charAt(0);
			}
			return ' ';
		}

		static String fill(char c, int n) {
			char[] cs = new char[n];
			Arrays.fill(cs, c);
			return new String(cs);
		}
	}

	@Deprecated
	@Func
	class ConcatFunc extends FelFunc<String> {
		@Override
		public String invoke(Object... args) {
			String result = "";
			for (Object a : args)
				result += a.toString();
			return result;
		}
	}
}
