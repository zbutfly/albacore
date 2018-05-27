package net.butfly.albacore.utils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.logging.Logger;

public class JVM {
	Logger logger = Logger.getLogger(parseMainClass().toString());
	private static final Object mutex = new Object();
	private static JVM current = null;
	final List<String> vmArgs;
	public final Class<?> mainClass;
	final boolean debugging;
	final List<String> args;

	private JVM() {
		this.mainClass = parseMainClass();
		this.vmArgs = java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments();
		this.debugging = checkDebug();
		this.args = parseArgs();
	}

	private JVM(Class<?> mainClass, String... args) {
		this.mainClass = mainClass;
		this.vmArgs = java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments();
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
		logger.warning("No sun.java.command property, main args could not be fetched.");
		return null;
	}

	private List<String> parseCommandLineArgs(String cmd) {
		// XXX
		return Arrays.asList(cmd.split(" "));
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
		if (!Refs.accessible(mm)) mm.setAccessible(true);
		logger.fine("Wrapped main method found and to be invoked: " + mm.toString());
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

	private static Class<?> parseMainClass() {
		Class<?> c = guessMainClassByMaven();
		if (null != c) return c;
		c = guessMainClassByJava();
		if (null != c) return c;
		throw new RuntimeException(new ClassNotFoundException("Can't find main class. It can be define manully by -Dexec.mainClass=..."));
	}

	static Class<?> guessMainClassByJava() {
		String[] argus = System.getProperty("sun.java.command").split("[\\s]+");
		if ("-jar".equals(argus[0])) {
			try (JarFile jar = new JarFile(Thread.currentThread().getContextClassLoader().getResource(argus[1]).getPath());) {
				String mn = jar.getManifest().getMainAttributes().getValue(Attributes.Name.MAIN_CLASS);
				if (null != mn) try {
					return Class.forName(mn);
				} catch (ClassNotFoundException e) {}
			} catch (IOException e) {}
		} else try {
			return Class.forName(argus[0]);
		} catch (ClassNotFoundException e1) {}
		return null;
	}

	static Class<?> guessMainClassByMaven() {
		String n = System.getProperty("exec.mainClass");
		if (null != n) try {
			return Class.forName(n);
		} catch (ClassNotFoundException e) {}
		return null;
	}

	static Class<?> guessMainClassByThread() {
		Thread t = getMainThread();
		if (null == t) return null;
		StackTraceElement[] s = t.getStackTrace();
		try {
			return Class.forName(s[s.length - 1].getClassName());
		} catch (ClassNotFoundException e) {
			return null;
		}
	}

	private static Thread getMainThread() {
		for (Thread t : Thread.getAllStackTraces().keySet())
			if (t.getId() == 1) return t;
		return null;
	}

	public static Class<?> getCurrentClass(int offset) {
		try {
			return Class.forName(Thread.currentThread().getStackTrace()[2 - offset].getClassName());
		} catch (ClassNotFoundException e) {
			return null;
		}
	}
}