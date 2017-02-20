package net.butfly.albacore.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import net.butfly.albacore.utils.logger.Logger;

public class Configs extends Utils {
	private static final Logger logger = Logger.getLogger(Configs.class);
	private static final String MAIN_CONF_PROP_NAME = "albacore.config";
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

	private static final String DEFAULT_PROP_EXT = "." + System.getProperty("albacore.config.ext", "properties");

	/**
	 * Config definition priorities:
	 * <ol>
	 * <li>Systme Properties.
	 * <li>Config file: class-name.properties
	 * <ol>
	 * <li>File in runnable path.
	 * <li>File in classpath (based on classpath root).
	 * </ol>
	 * <li>System Variables.
	 * <li>XXXX-default.properties file in classpath of {@code loader}.
	 * <ol>
	 * 
	 * @return
	 */
	public static Map<String, String> loadAll(Class<?> loader, String filename) {
		logger.info("Load configs from file: " + filename + DEFAULT_PROP_EXT);
		Map<String, String> settings = new ConcurrentHashMap<>();
		fill(settings, mapProps(System.getProperties()), null, Configs::filter);
		try (InputStream in = IOs.openFile(filename + DEFAULT_PROP_EXT);) {
			if (!fill(settings, null, null, in)) {
				if (null != loader) try (InputStream in2 = IOs.openClasspath(loader, filename + DEFAULT_PROP_EXT);) {
					fill(settings, null, null, in2);
				} catch (IOException e) {}
			}
		} catch (IOException e) {}
		fill(settings, System.getenv(), s -> CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_DOT, s), Configs::filter);
		String defaultFile = loader.getPackage().toString().replaceAll("\\.", "/") + "/" + filename + DEFAULT_PROP_EXT;
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
		return fill(origin, mapProps(p), mapping, filter);
	}

	private static boolean fill(Map<String, String> origin, Map<String, String> next, Function<String, String> mapping,
			Predicate<String> filter) {
		if (null == next) return false;
		for (Entry<String, String> e : next.entrySet()) {
			if (filter.test(e.getKey())) continue;
			String key = mapping != null ? mapping.apply(e.getKey()) : e.getKey();
			origin.putIfAbsent(key, next.get(key));
		}
		return true;
	}

	private static String[] IGNORE_PREFIXS = new String[] { "java", "sun", "os", "user", "file", "path" };

	private static boolean filter(String key) {
		for (String ig : IGNORE_PREFIXS)
			if (ig.equals(key) || key.startsWith(ig)) return true;
		return (key.charAt(0) >= 'A' && key.charAt(0) <= 'Z');
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
