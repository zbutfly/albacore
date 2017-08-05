package net.butfly.albacore.utils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.google.common.base.Joiner;
import com.sun.akuma.Daemon;
import com.sun.akuma.JavaVMArguments;

import net.butfly.albacore.io.ext.OpenableThread;
import net.butfly.albacore.io.utils.Streams;
import net.butfly.albacore.utils.logger.Logger;
import sun.management.VMManagement;
import sun.misc.Signal;

@SuppressWarnings("restriction")
public final class Systems extends Utils {
	final static Logger logger = Logger.getLogger(Systems.class);

	public static void main(String... args) throws Throwable {
		System.err.println("Hello, world! ");
		int i = 0;
		for (String a : args)
			System.err.println("\targs[" + (++i) + "]: " + a);
	}

	public static void wrapMain(Class<?> mainClass, String... args) throws Throwable {
		BlockingQueue<Class<?>> cls = new LinkedBlockingQueue<>();
		cls.offer(mainClass);
		Method mainMethod = findMain(cls);
		if (null == mainMethod) throw new RuntimeException("No main method found in class [" + mainClass.getName() + "] and its parents.");
		if (!mainMethod.isAccessible()) mainMethod.setAccessible(true);
		logger.info("Wrapped main method found and to be invoked: " + mainMethod.toString());
		Object[] pargs = null == args ? new Object[0] : new Object[args.length];
		for (int i = 0; i < pargs.length; i++)
			pargs[i] = args[i];
		try {
			mainMethod.invoke(null, (Object) args);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			throw e.getTargetException();
		}
	}

	private static Method findMain(BlockingQueue<Class<?>> cls) {
		Class<?> c;
		while (null != (c = cls.poll())) {
			try {
				Method m = c.getDeclaredMethod("main", String[].class);
				if (Modifier.isStatic(m.getModifiers()) && Void.TYPE.equals(m.getReturnType())) return m;
			} catch (NoSuchMethodException | SecurityException e) {}
			Class<?> cc = c.getSuperclass();
			if (!Object.class.equals(cc)) cls.offer(cc);
			for (Class<?> ci : c.getInterfaces())
				cls.offer(ci);
		}
		return null;
	}

	public static void forkVM(boolean daemon) throws IOException {
		if (daemon) {
			Daemon d = new Daemon();
			if (d.isDaemonized()) try {
				d.init();
			} catch (Exception e) {
				throw new RuntimeException("JVM daemonized but init fail", e);
			}
			else {
				JavaVMArguments vmargs = vmargs();
				try {
					if (null == vmargs) d.daemonize();
					else d.daemonize(vmargs);
					System.exit(0);
				} catch (UnsupportedOperationException e) {
					logger.warn("JVM daemonized fail [" + e.getMessage() + "], continue running normally");
				}
			}
		} else {
			JavaVMArguments vmargs = vmargs();
			if (null == vmargs) return;
			try {
				Daemon.selfExec(vmargs);
				System.exit(0);
			} catch (UnsatisfiedLinkError e) {
				logger.warn("JVM args apply fail [" + e.getMessage() + "], continue running normally");
			}
		}
	}

	private static JavaVMArguments vmargs() {
		String vmargConf = System.getProperty("albacore.app.vmconfig");
		if (null == vmargConf) vmargConf = "vmargs.config";
		else vmargConf = "vmargs-" + vmargConf + ".config";
		logger.info("JVM args config file: [" + vmargConf
				+ "] reading...\n\t(default: vmargs.config, customized by -Dalbacore.app.vmconfig)");
		String[] confArgs;
		try (InputStream is = IOs.openFile(vmargConf);) {
			if (null == is) return null;
			confArgs = IOs.readLines(is, l -> l.matches("^\\s*(//|#).*"));
		} catch (IOException e1) {
			return null;
		}
		if (confArgs.length == 0) return null;
		// XXX: if return null, debug port duplicate between original and
		// forked.
		logger.debug("JVM args config file read: " + Joiner.on(" ").join(confArgs));
		JavaVMArguments origArgs;
		try {
			origArgs = JavaVMArguments.current();
			logger.info("JVM args original: " + origArgs);
		} catch (UnsupportedOperationException | IOException e) {
			logger.warn("JVM args fetching fail [" + e.getMessage() + "], continue running normally");
			return null;
		}
		NavigableSet<String> args = new ConcurrentSkipListSet<>(origArgs);
		for (String a : confArgs)
			if (args.add(a)) logger.info("JVM args append: " + a);
		Set<String> stripArgs = new HashSet<>();
		for (String a : args) {
			if (a.startsWith("-Dalbacore.app.")) stripArgs.add(a);
			else if (a.matches("^-Xrunjdwp:.*")) {
				Pattern p = Pattern.compile("address=(\\d+)");
				Matcher m = p.matcher(a);
				int port = Integer.parseInt(m.group(1));
				logger.info("Original debug port: " + port + ", fork in debug port: " + (++port) + ".");
				args.add(m.replaceAll("address=" + port));
				stripArgs.add(a);
			}
		}
		args.removeAll(stripArgs);
		logger.info("JVM args to apply: " + args.toString());
		return args.isEmpty() ? null : new JavaVMArguments(args);
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
			return Class.forName(System.getProperty("sun.java.command", ""));
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
			String suffix = System.getProperty("albacore.debug.suffix", "_DEBUG_" + new SimpleDateFormat("yyyyMMdd").format(new Date()));
			logger.warn("Debug mode, suffix [" + suffix + "] append to origin: [" + origin + "], now: [" + origin + suffix + "].");
			return origin + suffix;
		} else return origin;
	}

	public static void dryDebug(Runnable run, Logger logger, String info) {
		if (Systems.isDebug()) logger.warn("Debug mode, Dry run " + info + "!");
		else run.run();
	}

	private static final Map<String, BlockingQueue<Consumer<Signal>>> SIGNAL_HANDLERS = new ConcurrentHashMap<>();

	public static void handleSignal(Consumer<Signal> handler, String... signal) {
		for (String sig : signal)
			SIGNAL_HANDLERS.computeIfAbsent(sig, s -> {
				Signal.handle(new Signal(s), ss -> {
					BlockingQueue<Consumer<Signal>> handlers = SIGNAL_HANDLERS.get(ss.getName());
					logger.error(MessageFormat.format("Signal [{0}][{1}] caught, [{2}] handlers registered and will be invoking.", //
							ss.getName(), ss.getNumber(), handlers.size()));
					if (null != handlers) for (Consumer<Signal> h : handlers)
						h.accept(ss);
				});
				return new LinkedBlockingQueue<>();
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
