package net.butfly.albacore.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

import net.butfly.albacore.Albacore;

public final class Texts {
	public static boolean isEmpty(CharSequence str) {
		return null == str || str.toString().trim().length() == 0;
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

	private static final int POOL_SIZE = Integer.parseInt(System.getProperty(Albacore.Props.PROP_PARALLEL_POOL_SIZE_OBJECT, Integer
			.toString(Runtime.getRuntime().availableProcessors() - 1)));

	private static final Map<String, LinkedBlockingQueue<DateFormat>> DATE_FORMATS = new ConcurrentHashMap<>();
	public static final String SEGUST_DATE_FORMAT = "yyyy-MM-dd'T'hh:mm:ss.SSS'Z'";
	public static final String DEFAULT_DATE_FORMAT = System.getProperty(Albacore.Props.PROP_TEXT_DATE_FORMAT, SEGUST_DATE_FORMAT);

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

	public static List<String> split(String origin, String split) {
		List<String> l = new ArrayList<>();
		if (origin == null) return l;
		for (String s : origin.split(split)) {
			String ss = s.trim();
			if (!ss.isEmpty()) l.add(ss);
		}
		return l;
	}

	private static final Map<String, BlockingQueue<CloseDateFormat>> DATA_FORMATS = new ConcurrentHashMap<>();

	private static final class CloseDateFormat extends SimpleDateFormat implements AutoCloseable {
		private static final long serialVersionUID = 6278552096977426484L;

		public CloseDateFormat(String pattern) {
			super(pattern);
		}

		@Override
		public synchronized void close() {
			pool(toPattern()).offer(this);
		}
	}

	private static BlockingQueue<CloseDateFormat> pool(String format) {
		return DATA_FORMATS.compute(format, (k, q) -> null == q ? new LinkedBlockingQueue<CloseDateFormat>() : q);
	}

	private static CloseDateFormat fdate(String format) {
		BlockingQueue<CloseDateFormat> pool = pool(format);
		CloseDateFormat f;
		while (null == (f = pool.poll()))
			pool.offer(new CloseDateFormat(format));
		return f;
	}

	public static void format(Date date, String format, Consumer<String> using) {
		try (CloseDateFormat f = Texts.fdate(format);) {
			using.accept(f.format(date));
		}
	}
}
