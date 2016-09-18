package net.butfly.albacore.utils;

import static com.google.common.base.Preconditions.checkNotNull;

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

	CaseFormat(com.google.common.base.CaseFormat format) {
		this.format = format;
	}

	public final String to(CaseFormat dstCaseFormat, String str) {
		checkNotNull(dstCaseFormat);
		checkNotNull(str);
		return (dstCaseFormat == this) ? str : convert(dstCaseFormat, str);
	}

	String convert(CaseFormat dstCaseFormat, String str) {
		if (format == dstCaseFormat.format) return str;
		else if (format == null) return com.google.common.base.CaseFormat.LOWER_UNDERSCORE.to(dstCaseFormat.format, str.replaceAll("\\.",
				"_"));
		else if (dstCaseFormat.format == null) return format.to(com.google.common.base.CaseFormat.LOWER_UNDERSCORE, str).replaceAll("_",
				"\\.");
		else return format.to(dstCaseFormat.format, str);
	}
}
