package net.butfly.albacore.utils.logger;

import static net.butfly.albacore.Albacore.Props.PROP_PARALLEL_POOL_SIZE_OBJECT;

import java.text.DecimalFormat;
import java.util.concurrent.LinkedBlockingQueue;

final class StatsUtils {
	private static final int POOL_SIZE = Integer.parseInt(System.getProperty(PROP_PARALLEL_POOL_SIZE_OBJECT, //
			Integer.toString(Runtime.getRuntime().availableProcessors() - 1)));
	private static LinkedBlockingQueue<DecimalFormat> DEC_FORMATS = new LinkedBlockingQueue<>(POOL_SIZE);

	private static long K = 1024;
	private static long M = K * K;
	private static long G = M * K;
	private static long T = G * K;
	private static int SECOND = 1000;
	private static int MINUTE = 60 * SECOND;
	private static int HOUR = 60 * MINUTE;

	static String formatKilo(double d, String unit) {
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

	static String formatMillis(double millis) {
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

	static class Result {
		public final long packs;
		public final long bytes;
		public final long millis;

		public Result(long packs, long bytes, long millis) {
			super();
			this.packs = packs;
			this.bytes = bytes;
			this.millis = millis;
		}
	}
}
