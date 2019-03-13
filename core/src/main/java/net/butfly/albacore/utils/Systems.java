package net.butfly.albacore.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.file.FileSystems;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.sun.akuma.Daemon;
import com.sun.akuma.JavaVMArguments;

import net.butfly.albacore.Albacore;
import net.butfly.albacore.paral.Sdream;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;
import sun.misc.Signal;

public final class Systems extends Utils {
	final static Logger logger = Logger.getLogger(Systems.class);

	public static void main(String... args) throws Throwable {
		System.err.println("Hello world, I'm " + Systems.pid());
		int i = 0;
		for (String a : args)
			System.err.println("\targs[" + (++i) + "]: " + a);
	}

	public static Sdream<Thread> threadsRunning() {
		return Sdream.of(Thread.getAllStackTraces().keySet()).filter(t -> !t.isDaemon());
	}

	@Deprecated
	public static long pid8() {
		String[] segs = java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@", 2);
		return null == segs || segs.length == 0 ? -1 : Long.parseLong(segs[0]);
	}

	private static long pid = -1;

	public static long pid() {
		if (pid < 0) {
			try { // java 9
				Class<?> c = Class.forName("java.lang.ProcessHandle"); // return ProcessHandle.current().pid();
				pid = ((Long) c.getMethod("pid").invoke(c.getMethod("current").invoke(null))).longValue();
			} catch (Exception e) {// java 8
				pid = pid8();
			}
		}
		return pid;
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

	private static final Map<String, BlockingQueue<Consumer<Signal>>> SIGNAL_HANDLERS = Maps.of();

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

	public static void fork(JVM jvm, boolean daemon) throws IOException {
		if (jvm.debugging) {
			logger.warn("JVM in debug, not forked.");
			return;
		}
		if (daemon) {
			Daemon d = new Daemon();
			if (d.isDaemonized()) try {
				d.init();
			} catch (Exception e) {
				throw new RuntimeException("JVM daemonized but init fail", e);
			}
			else {
				List<String> vmas = loadVmArgsConfig(jvm);
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
			List<String> vmas = loadVmArgsConfig(jvm);
			vmas.add("-Dalbacore.app._forked=true");
			fork(jvm, vmas, Boolean.parseBoolean(System.getProperty(Albacore.Props.PROP_APP_FORK_HARD, "true")));
		}
	}

	private static void fork(JVM jvm, List<String> vmas, boolean hard) throws IOException {
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
			vmas.addAll(jvm.args);
			Runtime.getRuntime().exec(vmas.toArray(new String[vmas.size()]));
			System.exit(0);
		}
	}

	private static List<String> loadVmArgsConfig(JVM jvm) {
		List<String> configed = new ArrayList<>();
		for (String conf : System.getProperty(Albacore.Props.PROP_APP_FORK_VM_ARGS, "").split(","))
			configed.addAll(loadVmArgs(conf));
		List<String> jvmArgs = new ArrayList<>(jvm.vmArgs);
		if (configed.isEmpty()) return jvmArgs;
		if (logger.isDebugEnabled()) {
			StringBuilder info = new StringBuilder("JVM args original: " + jvmArgs + ", \n\tappending config loaded: " + Joiner.on(" ")
					.join(configed));
			logger.debug(info);
		}
		jvmArgs.addAll(configed);
		return jvmArgs.stream().filter(a -> !a.startsWith("-Dalbacore.app.")).collect(Collectors.toList());
	}

	private static List<String> loadVmArgs(String conf) {
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

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static CodeSource[] allCodeSources() {
		Field f;
		try {
			f = java.security.SecureClassLoader.class.getDeclaredField("pdcache");
		} catch (NoSuchFieldException | SecurityException e1) {
			return null;
		}
		f.setAccessible(true);
		Collection<ProtectionDomain> domains;
		try {
			domains = ((Map) f.get(Thread.currentThread().getContextClassLoader())).values();
		} catch (IllegalArgumentException | IllegalAccessException e) {
			return null;
		}
		List<CodeSource> css = new ArrayList<>();
		for (ProtectionDomain d : domains)
			css.add(d.getCodeSource());
		return css.toArray(new CodeSource[css.size()]);
	}

	public static File jar(File jarFile, File... files) throws IOException {
		if (null == files || files.length == 0) return null;
		logger.info("Create jar [" + jarFile + "] with: \n\t" + Arrays.toString(files));
		try (FileOutputStream fo = new FileOutputStream(jarFile); JarOutputStream jo = new JarOutputStream(fo, new Manifest());) {
			for (File f : files)
				writeJar(jo, f, "");
		}
		return jarFile;
	}

	private static void writeJar(JarOutputStream jo, File f, String relative) throws IOException {
		if (f == null || !f.exists()) return;
		if (f.isDirectory()) for (String n : f.list())
			writeJar(jo, f.toPath().resolve(n).toFile(), relative + "/" + n);
		else if (f.isFile()) {
			JarEntry e = new JarEntry(relative);
			e.setTime(f.lastModified());
			jo.putNextEntry(e);
			try (FileInputStream in = new FileInputStream(f);) {
				jo.write(IOs.readAll(in));
			}
		}
	}
}
