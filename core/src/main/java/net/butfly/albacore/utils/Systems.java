package net.butfly.albacore.utils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.FileSystems;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Joiner;
import com.sun.akuma.Daemon;
import com.sun.akuma.JavaVMArguments;

import net.butfly.albacore.Albacore;
import net.butfly.albacore.io.ext.OpenableThread;
import net.butfly.albacore.io.utils.Streams;
import net.butfly.albacore.utils.logger.Logger;
import sun.management.VMManagement;
import sun.misc.Signal;

@SuppressWarnings("restriction")
public final class Systems extends Utils {
	final static Logger logger = Logger.getLogger(Systems.class);

	public static void main(String... args) throws Throwable {
		System.err.println("Hello world, I'm " + Systems.pid());
		int i = 0;
		for (String a : args)
			System.err.println("\targs[" + (++i) + "]: " + a);
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
		return JVM.current().mainClass;
	}

	public static boolean isDebug() {
		return JVM.current().debugging;
	}

	public static String suffixDebug(String origin, Logger logger) {
		if (Systems.isDebug()) {
			String suffix = System.getProperty(Albacore.Props.PROP_DEBUG_SUFFIX, "_DEBUG_" + new SimpleDateFormat("yyyyMMdd").format(
					new Date()));
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

	public final static GC gc = gc(Long.parseLong(System.getProperty(Albacore.Props.PROP_GC_INTERVAL_MS, "0")));

	private static GC gc(long ms) {
		if (ms <= 0) return null;
		if (ms < 500) ms = 500;
		GC gc = new GC(ms);
		gc.start();
		Systems.handleSignal(sig -> gc.close(), "TERM", "INT");
		return gc;
	}

	public static String getDefaultCachePathBase() {
		return System.getProperty(Albacore.Props.PROP_CACHE_LOCAL_PATH, "./cache/");
	}

	public static long sizeOf(Object obj) {
		try {
			return jdk.nashorn.internal.ir.debug.ObjectSizeCalculator.getObjectSize(obj);
		} catch (Exception ex) {
			return 0;
		}
	}

	public static class JVM {
		private final static Logger logger = Logger.getLogger(JVM.class);
		private static final Object mutex = new Object();
		private static JVM current = null;
		private final List<String> vmArgs;
		private final Class<?> mainClass;
		private final boolean debugging;
		private final List<String> args;

		private JVM() {
			this.mainClass = parseMainClass();
			this.vmArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
			this.debugging = checkDebug();
			this.args = parseArgs();
		}

		private JVM(Class<?> mainClass, String... args) {
			this.mainClass = mainClass;
			this.vmArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
			this.debugging = checkDebug();
			this.args = Arrays.asList(args);
		}

		public static JVM current() {
			synchronized (mutex) {
				if (null == current) current = new JVM();
				return current;
			}
		}

		public static JVM customize(String... args) {
			synchronized (mutex) {
				current = new JVM(null == current ? parseMainClass() : current.mainClass, args);
				return current;
			}
		}

		public static JVM customize(Class<?> mainClass, String... args) {
			synchronized (mutex) {
				current = new JVM(mainClass, args);
				return current;
			}
		}

		private List<String> parseArgs() {
			String cmd = System.getProperty("exec.mainArgs");
			if (null != cmd) return parseCommandLineArgs(cmd);
			cmd = System.getProperty("sun.java.command");
			if (null != cmd) {
				String[] segs = cmd.split(" ", 2);
				if (segs.length < 2) return Arrays.asList();
				else return parseCommandLineArgs(segs[1]);
			}
			logger.warn("No sun.java.command property, main args could not be fetched.");
			return null;
		}

		private List<String> parseCommandLineArgs(String cmd) {
			// XXX
			return Arrays.asList(cmd.split(" "));
		}

		private static Thread getMainThread() {
			for (Thread t : Thread.getAllStackTraces().keySet())
				if (t.getId() == 1) return t;
			return null;
		}

		private static Class<?> parseMainClass() {
			String n = System.getProperty("exec.mainClass");
			if (null != n) try {
				return Class.forName(n);
			} catch (ClassNotFoundException e) {}
			Thread t = getMainThread();
			if (null != t) {
				StackTraceElement[] s = t.getStackTrace();
				try {
					return Class.forName(s[s.length - 1].getClassName());
				} catch (ClassNotFoundException e) {}
			}
			n = System.getProperty("sun.java.command");
			if (null != n) {
				if (n.endsWith(".jar")) {
					try (JarFile jar = new JarFile(Thread.currentThread().getContextClassLoader().getResource(n).getPath());) {
						String mn = jar.getManifest().getMainAttributes().getValue(Attributes.Name.MAIN_CLASS);
						if (null != mn) try {
							return Class.forName(mn);
						} catch (ClassNotFoundException e) {}
					} catch (IOException e) {}
				} else try {
					return Class.forName(n.split(" ")[0]);
				} catch (ClassNotFoundException e) {}
			}
			return null;
		}

		public JVM unwrap() throws Throwable {
			if (args == null) throw new RuntimeException("JVM main class args not parsed, set it before run it.");
			if (args.isEmpty()) throw new RuntimeException(
					"JVM main method wrapping need at list 1 argument: actually main class being wrapped.");
			List<String> argl = new ArrayList<>(args);
			String mname = argl.remove(0);
			Class<?> mc;
			String mn = "main";
			if (mname.endsWith("(")) {
				int p = mname.lastIndexOf('.');
				mc = Class.forName(mname.substring(0, p));
				mn = mname.substring(p + 1, mname.length() - 2);
			} else mc = Class.forName(mname);
			String[] ma = argl.toArray(new String[0]);

			Method mm = findMain(mn, new LinkedBlockingQueue<>(Arrays.asList(mc)));
			if (null == mm) throw new RuntimeException("No main method found in class [" + mc.getName() + "] and its parents.");
			if (!mm.isAccessible()) mm.setAccessible(true);
			logger.debug("Wrapped main method found and to be invoked: " + mm.toString());
			try {
				mm.invoke(null, (Object) ma);
			} catch (IllegalAccessException e) {
				throw new RuntimeException(e);
			} catch (InvocationTargetException e) {
				throw e.getTargetException();
			}
			return this;
		}

		private static Method findMain(String methodName, BlockingQueue<Class<?>> cls) {
			Class<?> c;
			while (null != (c = cls.poll())) {
				try {
					Method m = c.getDeclaredMethod(methodName, String[].class);
					if (Modifier.isStatic(m.getModifiers()) && Void.TYPE.equals(m.getReturnType())) return m;
				} catch (NoSuchMethodException | SecurityException e) {}
				Class<?> cc = c.getSuperclass();
				if (!Object.class.equals(cc)) cls.offer(cc);
				for (Class<?> ci : c.getInterfaces())
					cls.offer(ci);
			}
			return null;
		}

		public JVM fork(boolean daemon) throws IOException {
			if (debugging) {
				logger.warn("JVM in debug, not forked.");
				return this;
			}
			if (daemon) {
				Daemon d = new Daemon();
				if (d.isDaemonized()) try {
					d.init();
				} catch (Exception e) {
					throw new RuntimeException("JVM daemonized but init fail", e);
				}
				else {
					List<String> vmas = loadVmArgsConfig();
					try {
						d.daemonize(new JavaVMArguments(vmas));
						System.exit(0);
					} catch (UnsupportedOperationException | UnsatisfiedLinkError e) {
						logger.warn("JVM daemonized fail [" + e.getMessage() + "], continue running normally");
					}
				}
			} else if (Boolean.parseBoolean(System.getProperty("albacore.app._forked", "false"))) //
				logger.debug("JVM detected to has been forked");
			else {
				List<String> vmas = loadVmArgsConfig();
				vmas.add("-Dalbacore.app._forked=true");
				fork(vmas, Boolean.parseBoolean(System.getProperty(Albacore.Props.PROP_APP_FORK_HARD, "true")));
			}
			return this;
		}

		protected void fork(List<String> vmas, boolean hard) throws IOException {
			if (null == vmas || vmas.size() == 0) return;
			if (hard) try {
				Daemon.selfExec(new JavaVMArguments(vmas));
				System.exit(0);
			} catch (UnsatisfiedLinkError e) {
				logger.warn("JVM args apply fail [" + e.getMessage() + "], continue running normally");
			}
			else {
				String jhome = System.getProperty("java.home");
				String java = "java";
				if (null != jhome) java = jhome + FileSystems.getDefault().getSeparator() + java;
				vmas.add(0, java);
				vmas.addAll(args);
				Runtime.getRuntime().exec(vmas.toArray(new String[vmas.size()]));
				System.exit(0);
			}
		}

		private List<String> loadVmArgsConfig() {
			List<String> configed = new ArrayList<>();
			for (String conf : System.getProperty(Albacore.Props.PROP_APP_FORK_VM_ARGS, "").split(","))
				configed.addAll(loadVmArgs(conf));
			List<String> jvmArgs = new ArrayList<>(vmArgs);
			if (configed.isEmpty()) return jvmArgs;
			logger.debug("JVM args original: " + jvmArgs + ", \n\tappending config loaded: " + Joiner.on(" ").join(configed));
			jvmArgs.addAll(configed);
			return jvmArgs.stream().filter(a -> !a.startsWith("-Dalbacore.app.")).collect(Collectors.toList());
		}

		private List<String> loadVmArgs(String conf) {
			String confFile;
			if (conf.isEmpty()) confFile = "vmargs.config";
			else confFile = "vmargs-" + conf + ".config";
			logger.debug("JVM args config file: [" + confFile
					+ "] reading...\n\t(default: vmargs.config, customized by -Dalbacore.app.vmconfig)");
			try (InputStream is = IOs.openFile(confFile);) {
				if (null == is) return new ArrayList<>();
				return Arrays.asList(IOs.readLines(is, l -> l.matches("^\\s*(//|#).*")));
			} catch (IOException e) {
				logger.error("JVM args config file: [" + confFile + "] read failed.", e);
				return new ArrayList<>();
			}

		}

		public String[] mainArgs() {
			if (null == args) throw new RuntimeException("JVM main class args not parsed, set it before run it.");
			return args.toArray(new String[args.size()]);
		}

		public List<String> vmArgs() {
			return vmArgs;
		}

		private boolean checkDebug() {
			for (String a : vmArgs)
				if (a.startsWith("-Xrunjdwp:") || a.startsWith("-agentlib:jdwp=")) return true;
			return false;
		}
	}

	public static class Timeing implements AutoCloseable {
		private final long begin;
		private final Logger tlogger;
		private final String prefix;

		public Timeing(Class<?> c, String prefix) {
			super();
			tlogger = Logger.getLogger(c);
			begin = tlogger.isDebugEnabled() ? System.currentTimeMillis() : 0;
			this.prefix = prefix;
		}

		@Override
		public void close() throws Exception {
			if (tlogger.isDebugEnabled()) tlogger.debug(prefix + " spent: " + (System.currentTimeMillis() - begin));
		}
	}
}
