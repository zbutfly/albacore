package net.butfly.albacore.utils;

import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import net.butfly.albacore.io.OpenableThread;
import net.butfly.albacore.lambda.Consumer;
import net.butfly.albacore.utils.logger.Logger;
import sun.misc.Signal;

@SuppressWarnings("restriction")
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

	public static void handleSignal(Consumer<Signal> handler, String... signal) {
		for (String s : signal)
			Signal.handle(new sun.misc.Signal(s), new sun.misc.SignalHandler() {
				@Override
				public void handle(sun.misc.Signal sig) {
					handler.accept(sig);
				}
			});
	}

	private static final OpenableThread GC_WATCHER = new OpenableThread() {
		@Override
		public void run() {
			setPriority(MAX_PRIORITY);
			long gccount = 0;
			this.setName("AlbacoreGCWatcher");
			logger.info(MessageFormat.format("GC manually watcher started, interval [{0}ms].", GC_INTERVAL));
			while (opened())
				try {
					sleep(GC_INTERVAL);
					System.gc();
					if ((++gccount) % 10 == 0) logger.trace("gc manually 10/" + gccount + " times.");
				} catch (InterruptedException e) {
					logger.warn(getName() + " interrupted.");
					return;
				}
			logger.info("GC manually watcher stopped.");
		}
	};

	private static long GC_INTERVAL = Long.parseLong(System.getProperty("albacore.manual.gc.interval", "1000"));

	public static void startGC() {
		GC_WATCHER.start();
	}

	public static void stopGC() {
		GC_WATCHER.close();
	}

	public static String getDefaultCachePath() {
		return System.getProperty("albacore.cache.local.path", "./cache/" + getMainClass().getSimpleName());
	}
}
