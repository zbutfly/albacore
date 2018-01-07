package net.butfly.albacore.utils;

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
	ConfigSet(Class<?> cl) {
		this(cl, null);
	}

	public ConfigSet(Class<?> cl, String prefix) {
		this.cls = Objects.requireNonNull(cl);
		String cname = Configs.calcClassConfigFile(cl);
		Config ann = findAnnedParent(cl);
		String pfx;
		if (Texts.notEmpty(prefix)) pfx = prefix;
		else pfx = null == ann ? null : ann.prefix();
		if (Config.NOT_DEFINE.equals(pfx)) pfx = null;
		String fname = null == ann ? null : ann.value();
		if (Texts.isEmpty(fname)) fname = cname;
		if (!fname.endsWith(Configs.DEFAULT_PROP_EXT)) fname += Configs.DEFAULT_PROP_EXT;
		String fnameDef = cl.getPackage().getName().replaceAll("\\.", "/");
		if (fnameDef.length() > 0) fnameDef += "/";
		fnameDef += cname + "-default" + Configs.DEFAULT_PROP_EXT;

		Map<String, String> settings = new ConcurrentHashMap<>();
		fill(settings, null, Configs::isKeyInvalid, mapProps(System.getProperties()));
		try (InputStream in = IOs.openFile(fname);) {
			if (!fill0(settings, in) && null != cl) try (InputStream in2 = IOs.openClasspath(cl, fname);) {
				fill0(settings, in2);
			} catch (IOException e) {}
		} catch (IOException e) {}
		fill(settings, s -> CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_DOT, s), Configs::isKeyInvalid, System.getenv());
		try (InputStream in = IOs.openClasspath(cl, fnameDef);) {
			fill0(settings, in);
		} catch (IOException e) {}
		Logger.getLogger(cl).info("Config class"//
				+ (null == pfx ? " with prefix [" + pfx + "]:" : ":")//
				+ "\n\tcustomized: [" + Paths.get("").toAbsolutePath().toString() + File.separator + fname + "]"//
				+ "\n\tcustomized: [classpath:/" + fname + "]" //
				+ "\n\t   default: [classpath:/" + fnameDef + "]");
		if (null != pfx && !pfx.endsWith(".")) pfx += ".";
		this.prefix = pfx;
		this.file = null;
		this.entries = settings;
	}

	ConfigSet(String filename, String pfx) {
		cls = null;
		Class<?> mcls = JVM.current().mainClass;
		if (!filename.endsWith(Configs.DEFAULT_PROP_EXT)) filename = filename + Configs.DEFAULT_PROP_EXT;
		Map<String, String> settings = new ConcurrentHashMap<>();
		fill(settings, null, Configs::isKeyInvalid, mapProps(System.getProperties()));
		try (InputStream in = IOs.openFile(filename);) {
			if (!fill0(settings, in)) try (InputStream in2 = IOs.openClasspath(mcls, filename);) {
				fill0(settings, in2);
			} catch (IOException e) {}
		} catch (IOException e) {}
		fill(settings, s -> CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_DOT, s), Configs::isKeyInvalid, System.getenv());
		Logger.getLogger(mcls).info("Config from file"//
				+ (null == pfx ? " with prefix [" + pfx + "]:" : ":")//
				+ "\n\tcustomized: [" + Paths.get("").toAbsolutePath().toString() + File.separator + filename + "]"//
				+ "\n\tcustomized: [classpath:/" + filename + "]" //
		);
		if (null != pfx && !pfx.endsWith(".")) pfx += ".";
		this.prefix = pfx;
		this.file = null;
		this.entries = settings;
	}

	public String get(String key) {
		return gets(keyWithPrefix(key));
	}

	public String get(String key, String... def) {
		return gets(keyWithPrefix(key), def);
	}

	public String gets(String key) {
		Configs.logger.debug("Config fetch by key [" + key + "]");
		return entries.get(key);
	}

	public String gets(String key, String... def) {
		Configs.logger.debug("Config fetch by key [" + key + "]");
		return entries.getOrDefault(key, first(def));
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

	public Map<String, String> getByPrefix(String prefix) {
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