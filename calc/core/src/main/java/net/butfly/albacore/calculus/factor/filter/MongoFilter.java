package net.butfly.albacore.calculus.factor.filter;

import java.util.regex.Pattern;

public interface MongoFilter extends Filter {
	public static final class Regex extends FieldFilter<String> implements MongoFilter {
		private static final long serialVersionUID = 4509935644112856045L;
		public Pattern regex;

		public Regex(String field, String regex) {
			super(field);
			this.regex = Pattern.compile(regex);
		}
	}

	public static final class Where extends SingleFieldFilter<String> implements MongoFilter {
		private static final long serialVersionUID = 44828739379557295L;

		public Where(String field, String where) {
			super(field, where);
		}

	}

	public static final class Type extends SingleFieldFilter<Integer> implements MongoFilter {
		private static final long serialVersionUID = 8237205268336851434L;

		public Type(String field, int type) {
			super(field, type);
		}
	}
}
