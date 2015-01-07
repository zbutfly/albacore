package net.butfly.albacore.logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.w3c.dom.Document;

class Log4jFactory {
	private Log4jFactory() {}

	private static final String[] customized_configs = new String[] { "log4j.xml", "log4j.properties" };
	private static final String[] internal_configs = new String[] { "/net/butfly/albacore/core/logger/log4j.xml" };
	private static boolean initialized = false;

	static {
		initializeLog4j();
	}

	static void initializeLog4j() {
		if (initialized) return;
		try {
			InputStream in = null;
			for (String conf : customized_configs) {
				try {
					in = Thread.currentThread().getContextClassLoader().getResourceAsStream(conf);
					if (in != null) return;
				} finally {
					if (in != null) try {
						in.close();
					} catch (IOException e) {}
				}
			}
			for (String conf : internal_configs) {
				try {
					in = Thread.currentThread().getContextClassLoader().getResourceAsStream(conf);
					if (conf.endsWith(".xml")) {
						Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(in);
						DOMConfigurator.configure(doc.getDocumentElement());
						return;
					} else if (conf.endsWith(".properties")) {
						Properties prop = new Properties();
						prop.load(in);
						PropertyConfigurator.configure(prop);
						return;
					}
				} catch (Exception ex) {} finally {
					if (in != null) try {
						in.close();
					} catch (IOException e) {}
				}
			}
		} finally {
			initialized = true;
		}
	}
}
