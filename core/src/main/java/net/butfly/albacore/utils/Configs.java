package net.butfly.albacore.utils;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.file.Path;
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
	public static final Conf MAIN = init(Systems.getMainClass());

	private static final String DEFAULT_PROP_EXT() {
		return "." + System.getProperty("albacore.config.ext", "properties");
	}

	public static Conf init() {
		return init(Systems.getMainClass());
	}

	/**
	 * Config definition priorities:
	 * <ol>
	 * <li>Systme Properties.
	 * <li>First found of: {@code filename}.properties
	 * <ol>
	 * <li>In executing path.
	 * <li>In classpath (based on classpath root).
	 * </ol>
	 * <li>System Variables.
	 * <li>{@code filename}-default.properties file in classpath of
	 * {@code loader}.
	 * <ol>
	 * 
	 * @param prefix
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 */
	public static Conf init(Class<?> cl) {
		Config c = cl.getAnnotation(Config.class);
		return null == c ? init(cl, calcClassConfigFile(cl), null) : init(cl, c.value(), c.prefix());
	}

	/**
	 * @param configPrefix
	 * @param cl
	 * @param conf
	 * @return
	 * @deprecated use {@code @Config} to define config file and prefix.
	 */
	@Deprecated
	public static Conf init(Class<?> cl, String filename, String prefix) {
		String ext = DEFAULT_PROP_EXT();
		String defname = cl.getPackage().getName().replaceAll("\\.", "/") + "/" + calcClassConfigFile(cl) + "-default" + ext;
		if (!filename.endsWith(ext)) filename = filename + ext;
		Logger logger = Logger.getLogger(cl);
		logger.info("Config class"//
				+ ((prefix != null && prefix.length() > 0) ? " with prefix [" + prefix + "]:" : ":")//
				+ "\n\tcustomized: [executable:/" + filename + "]"//
				+ "\n\tcustomized: [classpath:/" + filename + "]" //
				+ "\n\tdefault: [classpath:/" + defname + "]");
		Map<String, String> settings = new ConcurrentHashMap<>();
		fill(settings, null, Configs::filterSystemAndInvalidPrefix, mapProps(System.getProperties()));
		try (InputStream in = IOs.openFile(filename);) {
			if (!fill(settings, null, null, in) && null != cl) try (InputStream in2 = IOs.openClasspath(cl, filename);) {
				fill(settings, null, null, in2);
			} catch (IOException e) {}
		} catch (IOException e) {}
		fill(settings, s -> CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_DOT, s), Configs::filterSystemAndInvalidPrefix, System
				.getenv());
		try (InputStream in = IOs.openClasspath(cl, defname);) {
			fill(settings, null, null, in);
		} catch (IOException e) {}
		return new Conf(prefix, settings);
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

	private static String calcClassConfigFile(Class<?> configed) {
		return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, configed.getSimpleName());
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface Config {
		String value()

		default "";

		String prefix() default "";
	}

	public static final class Conf {
		protected final Path file; // TODO: hot reloading
		private final String prefix;
		private final Map<String, String> entries;

		public Conf(String prefix, Map<String, String> entries) {
			super();
			if (null == prefix || prefix.length() == 0) this.prefix = null;
			else {
				if (!prefix.endsWith(".")) prefix = prefix + ".";
				this.prefix = prefix;
			}
			this.file = null;
			this.entries = entries;
		}

		public final String get(String key) {
			return entries.get(null == prefix ? key : prefix + key);
		}

		public final String get(String key, String def) {
			return entries.getOrDefault(null == prefix ? key : prefix + key, def);
		}

		public boolean contains(String key) {
			return entries.containsKey(key);
		}

		public Conf prefix(String prefix) {
			return new Conf(prefix, entries);
		}
	}

	// other utils

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
