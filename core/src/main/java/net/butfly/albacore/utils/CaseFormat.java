package net.butfly.albacore.utils;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.GwtCompatible;

@GwtCompatible
public enum CaseFormat {
	NO_CHANGE(null),
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

	CaseFormat(com.google.common.base.CaseFormat format) {
		this.format = format;
	}

	public final String to(CaseFormat to, String str) {
		checkNotNull(to);
		checkNotNull(str);
		return (to == this || to == CaseFormat.NO_CHANGE) ? str : convert(to, str);
	}

	String convert(CaseFormat to, String str) {
		if (format == to.format) return str;
		else if (format == null) // LOWER_DOT
			return com.google.common.base.CaseFormat.LOWER_UNDERSCORE.to(to.format, str.replaceAll("\\.", "_"));
		else if (to.format == null) return format.to(com.google.common.base.CaseFormat.LOWER_UNDERSCORE, str).replaceAll("_", "\\.");
		else return format.to(to.format, str);
	}
}
