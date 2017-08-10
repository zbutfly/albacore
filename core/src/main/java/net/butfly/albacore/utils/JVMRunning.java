package net.butfly.albacore.utils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.FileSystems;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.sun.akuma.Daemon;
import com.sun.akuma.JavaVMArguments;

import net.butfly.albacore.utils.logger.Logger;

public class JVMRunning {
	private final static Logger logger = Logger.getLogger(JVMRunning.class);
	private static final Object mutex = new Object();
	private static JVMRunning current = null;
	private final List<String> vmArgs;
	public final Class<?> mainClass;
	public final boolean debugging;
	private final List<String> args;

	private JVMRunning() {
		this.mainClass = parseMainClass();
		this.vmArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
		this.debugging = checkDebug();
		this.args = parseArgs();
	}

	private JVMRunning(Class<?> mainClass, String... args) {
		this.mainClass = mainClass;
		this.vmArgs = ManagementFactory.getRuntimeMXBean().getInputArguments();
		this.debugging = checkDebug();
		this.args = Arrays.asList(args);
	}

	public static JVMRunning current() {
		synchronized (mutex) {
			if (null == current) current = new JVMRunning();
			return current;
		}
	}

	public static JVMRunning customize(String... args) {
		synchronized (mutex) {
			current = new JVMRunning(null == current ? parseMainClass() : current.mainClass, args);
			return current;
		}
	}

	public static JVMRunning customize(Class<?> mainClass, String... args) {
		synchronized (mutex) {
			current = new JVMRunning(mainClass, args);
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
				try (JarFile jar = new JarFile(Thread.currentThread().getContextClassLoader().getResource("narc.jar").getPath());) {
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

	public JVMRunning unwrap() throws Throwable {
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

	public JVMRunning fork(boolean daemon) throws IOException {
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
			fork(vmas, Boolean.parseBoolean(System.getProperty("albacore.app.fork.hard", "true")));
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
		for (String conf : System.getProperty("albacore.app.vmconfig", "").split(","))
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