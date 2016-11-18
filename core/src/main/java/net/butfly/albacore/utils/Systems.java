package net.butfly.albacore.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

import net.butfly.albacore.lambda.Consumer;
import net.butfly.albacore.utils.logger.Logger;

public final class Systems extends Utils {
	public static Class<?> getMainClass() {
		try {
			return Class.forName(System.getProperty("sun.java.command"));
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
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
		else run.run();;
	}

	@SuppressWarnings("restriction")
	public static void handleSignal(Consumer<sun.misc.Signal> handler, String... signal) {
		for (String s : signal)
			sun.misc.Signal.handle(new sun.misc.Signal(s), new sun.misc.SignalHandler() {
				@Override
				public void handle(sun.misc.Signal sig) {
					handler.accept(sig);
				}
			});
	}
}
