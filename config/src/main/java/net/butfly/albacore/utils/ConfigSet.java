package net.butfly.albacore.utils;

import static net.butfly.albacore.utils.Texts.AnsiColor.BRIGHT;
import static net.butfly.albacore.utils.Texts.AnsiColor.FG_MAGENTA;
import static net.butfly.albacore.utils.logger.ANSIConsoleAppender.colorize;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.log4j.Level;

import net.butfly.albacore.utils.logger.Logger;

public final class ConfigSet {
	final Map<String, String> entries;

	protected final List<Path> file; // TODO: hot reloading
	private final String prefix;
	private final Class<?> cls;

	public boolean prefixed() {
		return null != prefix;
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
	 * <li>{@code filename}-default.properties file in classpath of {@code loader}.
	 * <ol>
	 * 
	 * @param prefix
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 */
	ConfigSet(Class<?> cl, String... prefix) {
		cls = Objects.requireNonNull(cl);

		String cname = Configs.calcClassConfigFile(cl);
		Config ann = findAnnedParent(cl);
		String fname = null == ann ? null : ann.value();
		if (Texts.isEmpty(fname)) fname = cname;
		if (!fname.endsWith(Configs.DEFAULT_PROP_EXT)) fname += Configs.DEFAULT_PROP_EXT;
		Package pkg = cl.getPackage();// on jdk8, root package return null
		String pn = null == pkg ? "" : pkg.getName();
		String fnameDef = pn.replaceAll("\\.", "/");
		if (fnameDef.length() > 0) fnameDef += "/";
		fnameDef += cname + "-default" + Configs.DEFAULT_PROP_EXT;

		String pfx;
		if (null != prefix && prefix.length > 0) pfx = String.join(".", prefix).replaceAll("\\.\\.", "\\.");
		else pfx = null == ann ? null : ann.prefix();
		if (Config.NOT_DEFINE.equals(pfx)) pfx = null;

		Map<String, String> settings = normalSettings(cl, fname);

		try (InputStream in = IOs.openClasspath(cl, fnameDef);) {
			if (fill0(settings, in)) settings.put("...CP.DEF", settings.get("") + "1");
		} catch (IOException e) {}
		if (null != pfx && !pfx.endsWith(".")) pfx += ".";
		Logger.getLogger(cl).info("Config class"//
				+ (null == pfx ? " with prefix [" + pfx + "]:" : ":")//
				+ "\n\tcustomized" + status(settings, "...CWD") + ": [" //
				+ Paths.get("").toAbsolutePath().toString() + File.separator + fname + "]"//
				+ "\n\tcustomized" + status(settings, "...CP") + ": [classpath:/" + fname + "]" //
				+ "\n\t   default" + status(settings, "...CP.DEF") + ": [classpath:/" + fnameDef + "]");
		this.prefix = pfx;
		this.file = null;
		this.entries = settings;
	}

	ConfigSet(String fname, Class<?> cl, String... prefix) {
		cls = Objects.requireNonNull(cl);

		if (!fname.endsWith(Configs.DEFAULT_PROP_EXT)) fname = fname + Configs.DEFAULT_PROP_EXT;

		String pfx;
		if (null != prefix && prefix.length > 0) pfx = String.join(".", prefix).replaceAll("\\.\\.", "\\.");
		else pfx = null;

		Map<String, String> settings = normalSettings(cl, fname);

		if (null != pfx && !pfx.endsWith(".")) pfx += ".";
		Logger.getLogger(cl).info("Config from file"//
				+ (null != pfx ? " with prefix [" + pfx + "]:" : ":")//
				+ "\n\tcustomized" + status(settings, "...CWD") + ": [" //
				+ Paths.get("").toAbsolutePath().toString() + File.separator + fname + "]"//
				+ "\n\tcustomized" + status(settings, "...CP") + ": [classpath:/" + fname + "]" //
		);
		this.prefix = pfx;
		this.file = null;
		this.entries = settings;
	}

	ConfigSet(String fname, String... prefix) {
		this(fname, JVM.current().mainClass, prefix);
	}

	private static String status(Map<String, String> s, String k) {
		return "1".equals(s.remove(k)) ? "(loaded)   " : "(not found)";
	}

	private static Map<String, String> normalSettings(Class<?> cl, String fname) {
		Map<String, String> settings = new ConcurrentHashMap<>();
		fill(settings, null, Configs::isKeyInvalid, mapProps(System.getProperties()));
		try (InputStream in = IOs.openFile(fname);) {
			if (fill0(settings, in)) settings.put("...CWD", "1");
			else try (InputStream in2 = IOs.openClasspath(cl, fname);) {
				if (fill0(settings, in2)) settings.put("...CP", "1");
			} catch (IOException e) {}
		} catch (IOException e) {}
		fill(settings, s -> CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_DOT, s), Configs::isKeyInvalid, System.getenv());
		return settings;
	}

	public String get(String key) {
		return gets(keyWithPrefix(key));
	}

	public String get(String key, String... def) {
		return gets(keyWithPrefix(key), def);
	}

	String gets(String key) {
		String v = entries.get(key);
		Configs.logger.info("Config item [" + key + "], returned: [" + v + "].");
		return v;
	}

	public String getss(String comments, String def, String... keys) {
		StringBuilder info = new StringBuilder("Config item [" + String.join(", ", keys) + "]");
		if (null != comments && comments.length() > 0) info.append(": ").append(colorize(comments, Level.INFO, BRIGHT, FG_MAGENTA)).append(
				"\n\t");
		String v;
		if (null != def) info.append(" default: [" + def + "]");
		for (String k : keys)
			if (null != (v = entries.get(k))) {
				Configs.logger.info(info + " found: [" + v + "].");
				return v;
			}
		info.append(" not found").append(null == def ? "." : " and return: [" + def + "].");

		Configs.logger.info(info);
		return def;
	}

	@Deprecated
	String gets(String key, String... def) {
		String v = entries.getOrDefault(key, first(def));
		Configs.logger.info("Config item [" + key + "] with default value [" + String.join(", ", def) + "]"//
				+ ", returned: [" + v + "].");
		return v;
	}

	public String getn(String priority, String key) {
		return Texts.isEmpty(priority) ? get(key) : priority;
	}

	public String getn(String priority, String key, String... def) {
		return Texts.isEmpty(priority) ? get(key, def) : priority;
	}

	private String first(String... def) {
		for (String s : def)
			if (null != s) return s;
		return null;
	}

	public boolean has(String key) {
		return entries.containsKey(key);
	}

	public ConfigSet prefix(String prefix) {
		return new ConfigSet(prefixed() ? this.prefix + prefix : prefix, entries);
	}

	public Map<String, String> prefixed(String prefix) {
		Configs.logger.debug("Config sub fetch by prefix [" + prefix + "]");
		Map<String, String> sub = new ConcurrentHashMap<>();
		for (String k : entries.keySet())
			if (k.startsWith(prefix)) {
				String subk = k.substring(prefix.length() - 1);
				while (!subk.isEmpty() && subk.startsWith("."))
					subk = subk.substring(1);
				sub.put(subk, entries.get(k));
			}
		return sub;
	}

	// ==================================
	private ConfigSet(String prefix, Map<String, String> entries) {
		cls = null;
		if (Texts.isEmpty(prefix)) this.prefix = null;
		else this.prefix = prefix.endsWith(".") ? prefix : prefix + ".";
		this.file = null;
		this.entries = entries;
	}

	private String keyWithPrefix(String key) {
		return prefixed() ? prefix + key : key;
	}

	private static boolean fill0(Map<String, String> origin, InputStream next) {
		if (null == next) return false;
		Properties p = new Properties();
		try {
			p.load(next);
		} catch (Exception e) {
			return false;
		}
		return fill(origin, null, null, mapProps(p));
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

	private static Map<String, String> mapProps(Properties props) {
		Map<String, String> kvs = new ConcurrentHashMap<>();
		for (String k : props.stringPropertyNames()) {
			if (Configs.isKeyInvalid(k)) continue;
			String v = props.getProperty(k);
			if (null == v) continue;
			v = v.trim();
			if (v.length() == 0) continue;
			kvs.put(k, props.getProperty(k));
		}
		return kvs;
	}

	private static Config findAnnedParent(Class<?> cl) {
		if (cl == null) return null;
		if (cl.isAnnotationPresent(Config.class)) return cl.getAnnotation(Config.class);
		Config c;
		if (null != (c = findAnnedParent(cl.getSuperclass()))) return c;
		for (Class<?> cc : cl.getInterfaces())
			if (null != (c = findAnnedParent(cc.getSuperclass()))) return c;
		return null;
	}

	@Override
	public String toString() {
		StringBuilder s = new StringBuilder("ConfigSet ");
		if (null != prefix) s.append("[prefix: " + prefix + "]");
		if (null != cls) s.append("[class: " + cls.toString() + "]");
		s.append(" {");
		for (String key : entries.keySet())
			s.append("\n\t").append(key).append(": ").append(entries.get(key));
		return s.append("\n}").toString();
	}
}