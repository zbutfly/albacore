package net.butfly.albacore.expr.fel;

import static net.butfly.albacore.expr.fel.Fels.NULL;
import static net.butfly.albacore.expr.fel.Fels.isNull;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import net.butfly.albacore.expr.fel.FelFunc.Func;
import net.butfly.albacore.utils.collection.Colls;
import net.butfly.albacore.utils.collection.Maps;

public interface FuncForStr {
	/**
	 * strlen(str)：长度
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
	 */
	@Func
	class UuidFunc extends FelFunc<Object> {
		@Override
		public Object invoke(Object... args) {
			return UUID.randomUUID().toString();
		}
	}

	/**
	 * str2l('1234567')
	 */
	@Func
	class Str2lFun extends FelFunc<Long> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1;
		}

		@Override
		public Long invoke(Object... args) {
			return isNull(args[0]) ? 0 : Long.parseLong(args[0].toString());
		}
	}

	/**
	 * strrev('1234567')
	 */
	@Func
	class StrrevFunc extends FelFunc<String> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1;
		}

		@Override
		public String invoke(Object... args) {
			return isNull(args[0]) ? null : new StringBuilder(args[0].toString()).reverse().toString();
		}
	}

	/**
	 * case(value, case1, result1, case2, result2, ... [default])
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
			return NULL; // no matchs and no default
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
			return isNull(args[0]) ? null : args[0].toString().substring((int) args[1], (int) args[2]);
		}
	}

	/**
	 * strpadl(str, len, char)：左填充字符c直到结果字符串长度为l
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

	/**
	 * trim(str)：去除前后空格
	 */
	@Func
	class TrimFunc extends FelFunc<String> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1;
		}

		@Override
		public String invoke(Object... args) {
			return isNull(args[0]) ? null : args[0].toString().trim();
		}
	}

	/**
	 * replace(inputString, oldcharArr, newcharArr)：替换字符串中的特定字符
	 */
	@Func
	class ReplaceFunc extends FelFunc<String> {
		@Override
		protected boolean valid(int argl) {
			return argl == 3;
		}

		@Override
		public String invoke(Object... args) {
			if (isNull(args[0])) return null;
			if (isNull(args[1])) return args[0].toString();
			String r = isNull(args[2]) ? args[2].toString() : "";
			return args[0].toString().replaceAll(args[1].toString(), r);
		}
	}

	/**
	 * triml(str)：去除字符串前面部份空格或者tab
	 */
	@Func
	class TrimlFunc extends FelFunc<String> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1;
		}

		@Override
		public String invoke(Object... args) {
			if (isNull(args[0])) return null;
			String s = args[0].toString();
			int i = 0;
			for (; i < s.length(); i++) {
				char c = s.charAt(i);
				if (c != '\t' && c != ' ') break;
			}
			return s.substring(i);
		}
	}

	/**
	 * trimr(str)：去除字符串后面部份空格或者tab
	 */
	@Func
	class TrimrFunc extends FelFunc<String> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1;
		}

		@Override
		public String invoke(Object... args) {
			if (isNull(args[0])) return null;
			String s = args[0].toString();
			int i = s.length() - 1;
			for (; i >= 0; i--) {
				char c = s.charAt(i);
				if (c != '\t' && c != ' ') break;
			}
			return s.substring(0, i + 1);
		}
	}

	/**
	 * upper(str)：字符串转大写
	 */
	@Func
	class UpperFunc extends FelFunc<String> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1;
		}

		@Override
		public String invoke(Object... args) {
			return isNull(args[0]) ? null : args[0].toString().toUpperCase();
		}
	}

	/**
	 * lower(str)：字符串转小写
	 */
	@Func
	class LowerFunc extends FelFunc<String> {
		@Override
		protected boolean valid(int argl) {
			return argl == 1;
		}

		@Override
		public String invoke(Object... args) {
			return isNull(args[0]) ? null : args[0].toString().toLowerCase();
		}
	}

	/**
	 * split(str, splitter)：字符串切分
	 */
	@Func
	class SplitFunc extends FelFunc<List<String>> {
		@Override
		protected boolean valid(int argl) {
			return argl == 2;
		}

		@Override
		public List<String> invoke(Object... args) {
			if (isNull(args[0])) return null;
			String s = args[0].toString();
			if (isNull(args[1])) return Colls.list(s);
			return Colls.list(s.split(args[1].toString()));
		}
	}

	@Deprecated
	@Func
	class ConcatFunc extends FelFunc<String> {
		@Override
		public String invoke(Object... args) {
			StringBuilder result = new StringBuilder();
			for (Object a : args)
				result.append(a);
			return result.toString();
		}
	}
}
