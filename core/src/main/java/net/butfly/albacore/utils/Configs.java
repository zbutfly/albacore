package net.butfly.albacore.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import net.butfly.albacore.utils.logger.Logger;

public class Configs extends Utils {
	private static final Logger logger = Logger.getLogger(Configs.class);
	private static final String MAIN_CONF_PROP_NAME = "albacore.config";
	private static final String DEFAULT_PROP_EXT = "." + System.getProperty("albacore.config.ext", "properties");
	public static final Map<String, String> MAIN_CONF = load();

	public static void setConfig(String config) {
		if (null != config) System.setProperty(MAIN_CONF_PROP_NAME, config);
		else System.clearProperty(MAIN_CONF_PROP_NAME);
	}

	private static Map<String, String> load() {
		String configFile = System.getProperty(MAIN_CONF_PROP_NAME);
		if (null == configFile) {
			Class<?> c = Systems.getMainClass();
			logger.info("Load configs for class: " + c.getName());
			return load(c);
		} else {
			logger.info("Load configs for -D" + MAIN_CONF_PROP_NAME + "=" + configFile);
			return loadAll(Systems.getMainClass(), configFile);
		}
	}

	public static Map<String, String> load(Class<?> configed) {
		return loadAll(configed, CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, configed.getSimpleName()));
	}

	/**
	 * Config definition priorities:
	 * <ol>
	 * <li>Systme Properties.
	 * <li>First found of: {@code filename}.properties
	 * <ol>
	 * <li>In runnable path.
	 * <li>In classpath (based on classpath root).
	 * </ol>
	 * <li>System Variables.
	 * <li>{@code filename}-default.properties file in classpath of
	 * {@code loader}.
	 * <ol>
	 * 
	 * @return
	 */
	public static Map<String, String> loadAll(Class<?> loader, String filename) {
		logger.info("Load configs from file: " + filename + DEFAULT_PROP_EXT);
		if (filename.endsWith(DEFAULT_PROP_EXT)) filename = filename.substring(0, filename.length() - DEFAULT_PROP_EXT.length());
		Map<String, String> settings = new ConcurrentHashMap<>();
		fill(settings, null, Configs::filterSystemAndInvalidPrefix, mapProps(System.getProperties()));
		try (InputStream in = IOs.openFile(filename + DEFAULT_PROP_EXT);) {
			if (!fill(settings, null, null, in) && null != loader) try (InputStream in2 = IOs.openClasspath(loader, filename
					+ DEFAULT_PROP_EXT);) {
				fill(settings, null, null, in2);
			} catch (IOException e) {}
		} catch (IOException e) {}
		fill(settings, s -> CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_DOT, s), Configs::filterSystemAndInvalidPrefix, System
				.getenv());
		String defaultFile = loader.getPackage().getName().replaceAll("\\.", "/") + "/" + filename + "-default" + DEFAULT_PROP_EXT;
		logger.info("Load configs from default classpath file: " + defaultFile);
		if (null != loader) try (InputStream in = IOs.openClasspath(loader, defaultFile);) {
			fill(settings, null, null, in);
		} catch (IOException e) {}
		return settings;
	}

	private static boolean fill(Map<String, String> origin, Function<String, String> mapping, Predicate<String> filter, InputStream next) {
		if (null == next) return false;
		Properties p = new Properties();
		try {
			p.load(next);
		} catch (Exception e) {
			return false;
		}
		return fill(origin, mapping, filter, mapProps(p));
	}

	private static boolean fill(Map<String, String> origin, Function<String, String> mapping, Predicate<String> filter,
			Map<String, String> defaults) {
		if (null == defaults) return false;
		for (String key : defaults.keySet()) {
			if (null == defaults.get(key)) continue;
			String k = null == mapping ? key : mapping.apply(key);
			if (null == filter || !filter.test(k)) origin.putIfAbsent(k, defaults.get(key));
		}
		return true;
	}

	private static boolean filterSystemAndInvalidPrefix(String key) {
		char first = key.charAt(0);
		if (first < 'a' || first > 'z') return true;
		List<String> igs = Arrays.asList("java", "sun", "os", "user", "file", "path", "awt", "line", "home", "hostname", "shell", "lang");
		for (String i : igs)
			if (i.equals(key) || key.startsWith(i + ".")) return true;
		return false;
	}

	public static Map<String, String> mapProps(Properties props) {
		return props.entrySet().stream().filter(e -> e.getKey() != null && CharSequence.class.isAssignableFrom(e.getKey().getClass()) && e
				.getValue() != null && CharSequence.class.isAssignableFrom(e.getValue().getClass())).collect(Collectors.toConcurrentMap(
						e -> e.getKey().toString(), e -> e.getValue().toString()));
	}

	public static Properties propsMap(Map<String, String> settings) {
		Properties props = new Properties();
		settings.forEach((k, v) -> {
			if (null != k && null != v) props.setProperty(k, v);
		});
		return props;
	}
}
