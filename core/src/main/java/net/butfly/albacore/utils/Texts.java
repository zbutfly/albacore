package net.butfly.albacore.utils;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class Texts extends Utils {
	public static boolean isEmpty(String str) {
		return null == str || str.trim().length() == 0;
	}

	public static boolean notEmpty(String... str) {
		for (String s : str)
			if (isEmpty(s)) return false;
		return true;
	}

	public static String orNull(String str) {
		return notEmpty(str) ? str : null;
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

	private static final int POOL_SIZE = Integer.parseInt(Configs.gets("albacore.parallel.object.cache.size", Integer.toString(Runtime
			.getRuntime().availableProcessors() - 1)));

	private static final Map<String, LinkedBlockingQueue<DateFormat>> DATE_FORMATS = new ConcurrentHashMap<>();
	public static final String SEGUST_DATE_FORMAT = "yyyy-MM-dd'T'hh:mm:ss.SSSZ";
	public static final String DEFAULT_DATE_FORMAT = Configs.gets("albacore.text.date.format", SEGUST_DATE_FORMAT);

	public static String formatDate(Date date) {
		return formatDate(DEFAULT_DATE_FORMAT, date);
	}

	public static String formatDate(String format, Date date) {
		LinkedBlockingQueue<DateFormat> cache = DATE_FORMATS.computeIfAbsent(format, f -> new LinkedBlockingQueue<>(POOL_SIZE));
		DateFormat f = cache.poll();
		if (null == f) f = new SimpleDateFormat(format);
		try {
			return f.format(date);
		} finally {
			cache.offer(f);
		}
	}

	public static Date parseDate(String date) throws ParseException {
		return parseDate(DEFAULT_DATE_FORMAT, date);
	}

	public static Date parseDate(String format, String date) throws ParseException {
		LinkedBlockingQueue<DateFormat> cache = DATE_FORMATS.computeIfAbsent(format, f -> new LinkedBlockingQueue<>(POOL_SIZE));
		DateFormat f = cache.poll();
		if (null == f) f = new SimpleDateFormat(format);
		try {
			return f.parse(date);
		} finally {
			cache.offer(f);
		}
	}

	public static Map<String, String> parseQueryParams(String query) {
		Map<String, String> params = new HashMap<>();
		if (query != null) for (String param : query.split("&")) {
			String[] kv = param.split("=", 2);
			params.put(kv[0], kv.length > 1 ? kv[1] : null);
		}
		return params;
	}

	private static LinkedBlockingQueue<DecimalFormat> DEC_FORMATS = new LinkedBlockingQueue<>(POOL_SIZE);

	private static long K = 1024;
	private static long M = K * K;
	private static long G = M * K;
	private static long T = G * K;

	public static String formatKilo(double d, String unit) {
		DecimalFormat f = DEC_FORMATS.poll();
		if (null == f) f = new DecimalFormat("#.##");
		try {
			// double d = n;
			if (d > T) return f.format(d / T) + "T" + unit;
			// +"+" + formatBytes(bytes % T);
			if (d > G) return f.format(d / G) + "G" + unit;
			// +"+" + formatBytes(bytes % G);
			if (d > M) return f.format(d / M) + "M" + unit;
			// +"+" + formatBytes(bytes % M);
			if (d > K) return f.format(d / K) + "K" + unit;
			// +"+" + formatBytes(bytes % K);
			return f.format(d) + unit;
		} finally {
			DEC_FORMATS.offer(f);
		}
	}

	private static int SECOND = 1000;
	private static int MINUTE = 60 * SECOND;
	private static int HOUR = 60 * MINUTE;

	public static String formatMillis(double millis) {
		DecimalFormat f = DEC_FORMATS.poll();
		if (null == f) f = new DecimalFormat("#.##");
		try {
			if (millis > HOUR) return f.format(millis / HOUR) + " Hours";
			// + "+" + formatMillis(millis % HOUR);
			if (millis > MINUTE) return f.format(millis / MINUTE) + " Minutes";
			// + "+" + formatMillis(millis % MINUTE);
			if (millis > SECOND) return f.format(millis / SECOND) + " Secs";
			// + "+" + formatMillis(millis % SECOND);
			return f.format(millis) + " MS";
		} finally {
			DEC_FORMATS.offer(f);
		}
	}

	public static List<String> split(String origin, String split) {
		if (origin == null) return new ArrayList<>();
		return Stream.of(origin.split(split)).map(s -> s.trim()).filter(s -> !"".equals(s)).collect(Collectors.toList());
	}
}
