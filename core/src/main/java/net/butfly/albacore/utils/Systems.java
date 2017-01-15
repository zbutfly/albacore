package net.butfly.albacore.utils;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.io.OpenableThread;
import net.butfly.albacore.lambda.Consumer;
import net.butfly.albacore.utils.logger.Logger;
import sun.misc.Signal;

@SuppressWarnings({ "restriction" })
public final class Systems extends Utils {
	final static Logger logger = Logger.getLogger(Systems.class);

	public static Class<?> getMainClass() {
		try {
			return Class.forName(System.getProperty("sun.java.command"));
		} catch (ClassNotFoundException e) {
			StackTraceElement[] s = Thread.currentThread().getStackTrace();
			try {
				return Class.forName(s[s.length - 1].getClassName());
			} catch (ClassNotFoundException ee) {
				throw new RuntimeException(ee);
			}
		}
	}

	public static boolean isDebug() {
		return Boolean.parseBoolean(System.getProperty("albacore.debug"));
	}

	public static String suffixDebug(String origin, Logger logger) {
		if (Systems.isDebug()) {
			String suffix = "_DEBUG" + new SimpleDateFormat("yyyyMMdd").format(new Date());
			logger.warn("Debug mode, suffix [" + suffix + "] append to origin: [" + origin + "], now: [" + origin + suffix + "].");
			return origin + suffix;
		} else return origin;
	}

	public static void dryDebug(Runnable run, Logger logger, String info) {
		if (Systems.isDebug()) logger.warn("Debug mode, Dry run " + info + "!");
		else run.run();
	}

	private static final Map<String, List<Consumer<Signal>>> SIGNAL_HANDLERS = new ConcurrentHashMap<>();

	public static void handleSignal(Consumer<Signal> handler, String... signal) {
		for (String sig : signal)
			SIGNAL_HANDLERS.computeIfAbsent(sig, s -> {
				Signal.handle(new Signal(s), ss -> {
					List<Consumer<Signal>> handlers = SIGNAL_HANDLERS.get(ss.getName());
					logger.error(MessageFormat.format("Signal [{0}][{1}] caught, [{2}] handlers registered and will be invoking.", //
							ss.getName(), ss.getNumber(), handlers.size()));
					if (null != handlers) for (Consumer<Signal> h : handlers)
						h.accept(ss);
				});
				return new ArrayList<>();
			}).add(handler);
	}

	static {
		long ms = Long.parseLong(System.getProperty("albacore.gc.interval.ms", "1000"));
		if (ms > 0) {
			if (ms < 500) {
				logger.warn("Manually gc interval less 500 ms and too small. reset to 500 ms");
				ms = 500;
			} else logger.info("Manually gc interval " + ms + " ms.");
			final long cms = ms;
			OpenableThread w = new OpenableThread() {
				@Override
				public void run() {
					setPriority(MAX_PRIORITY);
					setName("Albacore-GC-Watcher");
					while (opened())
						try {
							sleep(cms);
							System.gc();
						} catch (InterruptedException e) {
							logger.warn(getName() + " interrupted.");
							return;
						}
				}

				@Override
				public void close() {
					super.close();
					logger.info("AlbacoreGC watcher closed.");
				}
			};
			Systems.handleSignal(sig -> w.close(), "TERM", "INT");
			w.start();
		}
	}

	public static String getDefaultCachePathBase() {
		return System.getProperty("albacore.cache.local.path", "./cache/");
	}

	public static long sizeOf(Object obj) {
		return jdk.nashorn.internal.ir.debug.ObjectSizeCalculator.getObjectSize(obj);
	}
}
