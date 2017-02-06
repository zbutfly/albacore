package net.butfly.albacore.utils;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public final class Texts extends Utils {
	public static boolean isEmpty(String str) {
		return null == str || str.trim().length() > 0;
	}

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
		if (null == hexStr) return null;
		byte[] bts = new byte[hexStr.length() / 2];
		int i = 0;
		int j = 0;
		for (; j < bts.length; j++) {
			bts[j] = (byte) Integer.parseInt(hexStr.substring(i, i + 2), 16);
			i += 2;
		}
		return bts;
	}

	public static byte[] long2bytes(long... longValue) {
		if (null == longValue) return null;
		byte[] b = new byte[longValue.length * 8];
		for (int i = 0; i < longValue.length; i++) {
			long l = longValue[i];
			for (int j = 0; j < 8; j++) {
				b[i * 8 + j] = (byte) l;
				l = l >> 8;
			}
		}
		return b;
	}

	public static long bytes2long(byte[] bytes) {
		Objects.noneNull(bytes);
		if (bytes.length < 8) bytes = Arrays.copyOf(bytes, 8);
		long l = 0L;
		for (int i = 0; i < 8; i++)
			l += bytes[i] << (i * 8);
		return l;
	}

	private static final ThreadLocal<Map<String, DateFormat>> DATE_CACHE = new ThreadLocal<Map<String, DateFormat>>() {
		@Override
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

	public static Map<String, String> parseQueryParams(String query) {
		Map<String, String> params = new HashMap<>();
		if (query != null) for (String param : query.split("&")) {
			String[] kv = param.split("=", 2);
			params.put(kv[0], kv.length > 1 ? kv[1] : null);
		}
		return params;
	}

	private static DecimalFormat f = new DecimalFormat("#.##");

	private static long K = 1024;
	private static long M = K * K;
	private static long G = M * K;
	private static long T = G * K;

	public static String formatKilo(long n, String unit) {
		double d = n;
		if (d > T) return f.format(d / T) + "T" + unit;
		// +"+" + formatBytes(bytes % T);
		if (d > G) return f.format(d / G) + "G" + unit;
		// +"+" + formatBytes(bytes % G);
		if (d > M) return f.format(d / M) + "M" + unit;
		// +"+" + formatBytes(bytes % M);
		if (d > K) return f.format(d / K) + "K" + unit;
		// +"+" + formatBytes(bytes % K);
		return f.format(d) + unit;
	}

	private static int SECOND = 1000;
	private static int MINUTE = 60 * SECOND;
	private static int HOUR = 60 * MINUTE;

	public static String formatMillis(long millis) {
		double ms = millis * 1.0;
		if (ms > HOUR) return f.format(ms / HOUR) + " Hours";
		// + "+" + formatMillis(millis % HOUR);
		if (ms > MINUTE) return f.format(ms / MINUTE) + " Minutes";
		// + "+" + formatMillis(millis % MINUTE);
		if (ms > SECOND) return f.format(millis / SECOND) + " Secs";
		// + "+" + formatMillis(millis % SECOND);
		return f.format(ms) + " MS";
	}
}
