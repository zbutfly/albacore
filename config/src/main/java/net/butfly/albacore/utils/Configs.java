package net.butfly.albacore.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.Albacore;
import net.butfly.albacore.utils.logger.Logger;

public final class Configs {
	static String DEFAULT_PROP_EXT = "." + System.getProperty(Albacore.Props.PROP_CONFIG_EXTENSION, "properties");
	public static final Logger logger = Logger.getLogger(Configs.class);
	private static final Map<Class<?>, ConfigSet> CLS_CONF = new ConcurrentHashMap<>();

	public static ConfigSet of() {
		return of(JVM.current().mainClass);
	}

	public static ConfigSet of(Class<?> cls) {
		return CLS_CONF.computeIfAbsent(cls, ConfigSet::new);
	}

	public static ConfigSet of(Class<?> cls, String prefix) {
		return CLS_CONF.computeIfAbsent(cls, c -> new ConfigSet(c, prefix));
	}

	public static <T> ConfigSet of(String filename, String prefix) {
		return of(filename, JVM.current().mainClass, prefix);
	}

	public static <T> ConfigSet of(String filename, Class<?> cls, String prefix) {
		return CLS_CONF.computeIfAbsent(cls, c -> new ConfigSet(filename, c, prefix));
	}

	public static String get(String key) {
		return of().get(key);
	}

	public static String get(String key, String... def) {
		return of().get(key, def);
	}

	public static void sets(String key, String value) {
		of().sets(key, value);
	}

	public static String gets(String key) {
		return of().gets(key);
	}

	@SuppressWarnings("deprecation")
	public static String gets(String key, String... def) {
		return of().gets(key, def);
	}

	public static String getss(String comments, String def, String... keys) {
		return of().getss(comments, def, keys);
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

	public static Map<String, String> prefixize(String prefix) {
		return of().prefixed(prefix);
	}

	// ================================

	private static final String[] SYS_ENV_IGNORED = { //
			"awt", "file", "java", "jdk", "line", "os", "path", "sun", "user", //
			"android", "fp", "fps", "git", "grep", "hadoop", "home", "hostname", //
			"jre", "lang", "lc", "m2", "number.of", "openssl", "processor", "scala", //
			"shell", "spark", "ssh", "userdomain", "vbox" };

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

	private Configs() {}
}
