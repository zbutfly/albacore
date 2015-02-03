package net.butfly.albacore.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public final class Texts extends Utils {
	public static String byte2hex(byte[] data) {
		if (null == data) return null;
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < data.length; i++) {
			String temp = Integer.toHexString(((int) data[i]) & 0xFF);
			for (int t = temp.length(); t < 2; t++)
				sb.append("0");
			sb.append(temp);
		}
		return sb.toString();
	}

	public static byte[] hex2byte(String hexStr) {
		if (null == hexStr) { return null; }
		byte[] bts = new byte[hexStr.length() / 2];
		int i = 0;
		int j = 0;
		for (; j < bts.length; j++) {
			bts[j] = (byte) Integer.parseInt(hexStr.substring(i, i + 2), 16);
			i += 2;
		}
		return bts;
	}

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

	/**
	 * Join strings without any spliter
	 * 
	 * @param list
	 * @return
	 */
	public static String join(String... list) {
		StringBuilder sb = new StringBuilder();
		for (String tt : list)
			sb.append(tt);
		return sb.substring(0, sb.length()).toString();
	}

	public static String join(char split, String... list) {
		StringBuilder sb = new StringBuilder();
		for (String tt : list)
			sb.append(tt).append(split);
		return sb.substring(0, sb.length() - 1).toString();
	}

	public static String join(String split, String... list) {
		StringBuilder sb = new StringBuilder();
		for (String tt : list)
			sb.append(tt).append(split);
		return sb.substring(0, sb.length() - split.length()).toString();
	}
}
