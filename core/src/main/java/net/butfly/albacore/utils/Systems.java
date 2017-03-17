package net.butfly.albacore.utils;

import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import net.butfly.albacore.io.ext.OpenableThread;
import net.butfly.albacore.io.utils.Streams;
import net.butfly.albacore.lambda.Consumer;
import net.butfly.albacore.utils.logger.Logger;
import sun.management.VMManagement;
import sun.misc.Signal;

@SuppressWarnings({ "restriction" })
public final class Systems extends Utils {
	final static Logger logger = Logger.getLogger(Systems.class);

	public static void main(String... args) {
		System.err.println(threadsRunning());
	}

	public static Stream<Thread> threadsRunning() {
		return Streams.of(Thread.getAllStackTraces().keySet()).filter(t -> !t.isDaemon());
	}

	public static int pid() {
		VMManagement vm = Reflections.get(ManagementFactory.getRuntimeMXBean(), "jvm");
		Method m;
		try {
			m = vm.getClass().getDeclaredMethod("getProcessId");
		} catch (NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}
		boolean acc = m.isAccessible();
		m.setAccessible(true);
		try {
			return ((Integer) m.invoke(vm)).intValue();
		} catch (IllegalAccessException | IllegalArgumentException e) {
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e.getTargetException());
		} finally {
			m.setAccessible(acc);
		}
	}

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
					synchronized (handlers) {
						logger.error(MessageFormat.format("Signal [{0}][{1}] caught, [{2}] handlers registered and will be invoking.", //
								ss.getName(), ss.getNumber(), handlers.size()));
						if (null != handlers) for (Consumer<Signal> h : handlers)
							h.accept(ss);
					}
				});
				return new ArrayList<>();
			}).add(handler);
	}

	private static class GC extends OpenableThread {
		private final long cms;

		public GC(long cms) {
			super("AlbacoreGCer");
			logger.info("Full GC Manually every [" + cms + " ms].");
			this.cms = cms;
			setPriority(MIN_PRIORITY);
			setDaemon(true);
		}

		@Override
		protected void exec() {
			while (opened())
				try {
					sleep(cms);
					System.gc();
				} catch (InterruptedException e) {
					logger.warn(getName() + " interrupted.");
					return;
				}
		}
	}

	public final static GC gc = gc(Long.parseLong(System.getProperty("albacore.gc.interval.ms", "0")));

	private static GC gc(long ms) {
		if (ms <= 0) return null;
		if (ms < 500) ms = 500;
		GC gc = new GC(ms);
		gc.start();
		Systems.handleSignal(sig -> gc.close(), "TERM", "INT");
		return gc;
	}

	public static String getDefaultCachePathBase() {
		return System.getProperty("albacore.cache.local.path", "./cache/");
	}

	public static long sizeOf(Object obj) {
		return jdk.nashorn.internal.ir.debug.ObjectSizeCalculator.getObjectSize(obj);
	}
}
