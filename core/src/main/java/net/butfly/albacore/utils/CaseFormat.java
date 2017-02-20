package net.butfly.albacore.utils;

import com.google.common.annotations.GwtCompatible;

@GwtCompatible
public enum CaseFormat {
	/**
	 * Dotted variable naming convention, e.g., "lower.hyphen".
	 */
	LOWER_DOT(null),
	/**
	 * Hyphenated variable naming convention, e.g., "lower-hyphen".
	 */
	LOWER_HYPHEN(com.google.common.base.CaseFormat.LOWER_HYPHEN),

	/**
	 * C++ variable naming convention, e.g., "lower_underscore".
	 */
	LOWER_UNDERSCORE(com.google.common.base.CaseFormat.LOWER_UNDERSCORE),

	/**
	 * Java variable naming convention, e.g., "lowerCamel".
	 */
	LOWER_CAMEL(com.google.common.base.CaseFormat.LOWER_CAMEL),

	/**
	 * Java and C++ class naming convention, e.g., "UpperCamel".
	 */
	UPPER_CAMEL(com.google.common.base.CaseFormat.UPPER_CAMEL),

	/**
	 * Java and C++ constant naming convention, e.g., "UPPER_UNDERSCORE".
	 */
	UPPER_UNDERSCORE(com.google.common.base.CaseFormat.UPPER_UNDERSCORE);

	private com.google.common.base.CaseFormat format;

	public static CaseFormat parse(String str) {
		Objects.noneNull(str);
		if (str.matches("^[a-z]*$")) return LOWER_CAMEL;
		if (str.matches("^[a-z][A-Za-z]+")) return LOWER_CAMEL;
		if (str.matches("^[A-Z][A-Za-z]+")) return UPPER_CAMEL;
		if (str.matches("^[a-z_]+")) return LOWER_UNDERSCORE;
		if (str.matches("^[A-Z_]+")) return UPPER_UNDERSCORE;
		if (str.matches("^[a-z\\.]+")) return LOWER_DOT;
		if (str.matches("^[a-z\\-]+")) return LOWER_HYPHEN;
		return LOWER_CAMEL;
	}

	private CaseFormat(com.google.common.base.CaseFormat format) {
		this.format = format;
	}

	public final String to(CaseFormat to, String str) {
		if (format == to.format) return str;
		// LOWER_DOT
		if (format == null) return com.google.common.base.CaseFormat.LOWER_HYPHEN.to(to.format, str.replaceAll("\\.", "-"));
		if (to.format == null) return format.to(com.google.common.base.CaseFormat.LOWER_HYPHEN, str).replaceAll("-", "\\.");
		return format.to(to.format, str);
	}
}
