package net.butfly.albacore.utils;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.Attributes;
import java.util.jar.JarFile;

import net.butfly.albacore.Albacore;

public final class Configs {
	static String DEFAULT_PROP_EXT = "." + System.getProperty(Albacore.Props.PROP_CONFIG_EXTENSION, "properties");
	private static final Map<Class<?>, ConfigSet> CLS_CONF = new ConcurrentHashMap<>();

	public static <T> ConfigSet of() {
		return of(parseMainClass());
	}

	public static <T> ConfigSet of(Class<T> cls) {
		return CLS_CONF.computeIfAbsent(cls, ConfigSet::new);
	}

	public static <T> ConfigSet of(String filename) {
		return of(filename, null);
	}

	public static <T> ConfigSet of(String filename, String prefix) {
		return new ConfigSet(filename, prefix);
	}

	public static String get(String key) {
		return of().get(key);
	}

	public static Map<String, String> getByPrefix(String prefix) {
		return of().getByPrefix(prefix);
	}

	public static String get(String key, String... def) {
		return of().get(key, def);
	}

	public static String gets(String key) {
		return of().gets(key);
	}

	public static String gets(String key, String... def) {
		return of().gets(key, def);
	}

	public static String getn(String priority, String key) {
		return of().getn(priority, key);
	}

	public static String getn(String priority, String key, String... def) {
		return of().getn(priority, key, def);
	}

	public static boolean has(String key) {
		return of().has(key);
	}

	// ================================

	private static final String[] SYS_ENV_IGNORED = { //
			"os", "processor", "user", "file", "path", "line", "home", "hostname", "shell", "lang", "ssh", //
			"fps", "openssl", "grep", "vbox", "userdomain", "fp", "number.of", //
			"sun", "jdk", "java", "jre", "awt", "android", "lc", "scala", "hadoop", "spark", "git", "m2" };

	static boolean isKeyInvalid(String key) {
		char first = key.charAt(0);
		if (first < 'a' || first > 'z' || !key.contains(".")) return true;
		for (String i : SYS_ENV_IGNORED)
			if (i.equals(key) || key.startsWith(i + ".")) return true;
		return false;
	}

	static String calcClassConfigFile(Class<?> configed) {
		return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, configed.getSimpleName());
	}

	private static Thread getMainThread() {
		for (Thread t : Thread.getAllStackTraces().keySet())
			if (t.getId() == 1) return t;
		return null;
	}

	private static Class<?> MAIN_CLASS = null;

	static Class<?> mainClass() {
		if (null == MAIN_CLASS) MAIN_CLASS = parseMainClass();
		return MAIN_CLASS;
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
		if (null == n) return null;
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
		return null;
	}

	private Configs() {}
}
