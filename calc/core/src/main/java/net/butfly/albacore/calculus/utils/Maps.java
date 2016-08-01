package net.butfly.albacore.calculus.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

public final class Maps implements Serializable {
	private static final long serialVersionUID = -6515563529313964038L;

	public static Properties subprops(Properties props, String prefix, boolean reservePrefix) {
		Properties r = new Properties();
		for (String key : props.stringPropertyNames()) {
			if (!key.startsWith(prefix)) continue;
			String nkey = reservePrefix ? key : key.substring(prefix.length());
			r.setProperty(nkey, props.getProperty(key));
		}
		return r;
	}

	public static Map<String, Properties> aggrProps(Properties props) {
		return aggrProps(props, '.');
	}

	public static Map<String, Properties> aggrProps(Properties props, char splitter) {
		Map<String, Properties> map = new HashMap<>();
		for (String key : props.stringPropertyNames()) {
			int pos = key.indexOf(splitter);
			String mainkey = key.substring(0, pos);
			String subkey = key.substring(pos + 1, key.length());
			Properties subp = map.get(mainkey);
			if (null == subp) {
				subp = new Properties();
				map.put(mainkey, subp);
			}
			subp.setProperty(subkey, props.getProperty(key));
		}
		return map;
	}

	public static Properties fromEnv(String prefix, Function<String, String> keyConverter) {
		final Properties p = new Properties();
		for (Map.Entry<String, String> e : System.getenv().entrySet())
			if (e.getKey().startsWith(prefix)) p.put(keyConverter.apply(e.getKey()), e.getValue());
		return p;
	}

	public static Properties fromFile(String classpath) {
		final Properties props = new Properties();
		try {
			props.load(inputStream(classpath));
		} catch (IOException e) {
			throw new RuntimeException("Config file not found in classpath: " + classpath, e);
		}
		return props;
	}

	public static final InputStream inputStream(String file) throws FileNotFoundException, IOException {
		URL url = Thread.currentThread().getContextClassLoader().getResource(file);
		return null == url ? new FileInputStream(file) : url.openStream();
	}
}
