package net.butfly.albacore.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public final class FormatUtils extends UtilsBase {
	private final static ThreadLocal<Map<String, DateFormat>> DATE_CACHE = new ThreadLocal<Map<String, DateFormat>>() {
		protected Map<String, DateFormat> initialValue() {
			return new HashMap<String, DateFormat>();
		}
	};

	private static final String DEFAULT_FORMAT = "";

	public static DateFormat dateFormat() {
		return dateFormat(DEFAULT_FORMAT);
	}

	public static DateFormat dateFormat(String format) {
		Map<String, DateFormat> map = DATE_CACHE.get();
		DateFormat f = map.get(format);
		if (null != f) return f;
		f = new SimpleDateFormat(format);
		map.put(format, f);
		return f;
	}
}
