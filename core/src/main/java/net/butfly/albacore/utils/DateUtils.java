package net.butfly.albacore.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

public final class DateUtils extends UtilsBase {
	private final static ThreadLocal<DateFormat> FORMATERs = new ThreadLocal<DateFormat>() {
		protected DateFormat initialValue() {
			return new SimpleDateFormat("yyMMddHHmmssSSS");
		}
	};

	public static DateFormat formater() {
		return FORMATERs.get();
	}
}
