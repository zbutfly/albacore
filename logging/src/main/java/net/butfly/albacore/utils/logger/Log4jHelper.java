package net.butfly.albacore.utils.logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

interface Log4jHelper {
	static void initLog4J() {
		URL res = Thread.currentThread().getContextClassLoader().getResource("log4j.properties");
		if (null != res) {
			Logger.getLogger("net.butfly.albacore.utils.logger.Logger").warn("log4j.properties found in CLASSPATH [" + res.toString()
					+ "], but is not recommended. log4j.xml is recommended.");
			return;
		}
		res = Thread.currentThread().getContextClassLoader().getResource("log4j.xml");
		if (null == res) {
			res = Thread.currentThread().getContextClassLoader().getResource("net/butfly/albacore/utils/logger/log4j-default.xml");
			if (null == res) throw new IllegalArgumentException("default log4j.xml not found.");
			// org.apache.log4j.xml.DOMConfigurator.configure(load(res));
			System.setProperty("log4j.configuration", res.toString());
		}
	}

	static Element load(URL res) {
		try (InputStream is = res.openStream(); Reader r = new InputStreamReader(is)) {
			return DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new InputSource(r)).getDocumentElement();
		} catch (IOException | SAXException | ParserConfigurationException e) {
			throw new IllegalArgumentException(e);
		}
	}

	static void changeSet(String logNameFilter, String logNameFilterType, String targetOperation, String targetLogger, String targetLogLevel) {
		String containsFilter = "Contains";
		@SuppressWarnings("rawtypes")
		Enumeration loggers = LogManager.getCurrentLoggers();
		Map<String, Logger> loggersMap = new HashMap<>();
		Logger rootLogger = LogManager.getRootLogger();
		if (!loggersMap.containsKey(rootLogger.getName())) loggersMap.put(rootLogger.getName(), rootLogger);

		while (loggers.hasMoreElements()) {
			Logger logger = (Logger) loggers.nextElement();
			if (logNameFilter == null || logNameFilter.trim().length() == 0) loggersMap.put(logger.getName(), logger);
			else if (containsFilter.equals(logNameFilterType)) {
				if (logger.getName().toUpperCase().indexOf(logNameFilter.toUpperCase()) >= 0) loggersMap.put(logger.getName(), logger);
			} else {
				// Either was no filter in IF, contains filter in ELSE IF, or begins with in ELSE
				if (logger.getName().startsWith(logNameFilter)) loggersMap.put(logger.getName(), logger);
			}
		}
		Set<String> loggerKeys = loggersMap.keySet();
		String[] keys = loggerKeys.toArray(new String[loggerKeys.size()]);
		Arrays.sort(keys, String.CASE_INSENSITIVE_ORDER);
		for (String k : keys) {
			Logger logger = (Logger) loggersMap.get(k);
			// MUST CHANGE THE LOG LEVEL ON LOGGER BEFORE GENERATING THE LINKS AND THE
			// CURRENT LOG LEVEL OR DISABLED LINK WON'T MATCH THE NEWLY CHANGED VALUES
			if ("changeLogLevel".equals(targetOperation) && targetLogger.equals(logger.getName())) {
				Logger selectedLogger = (Logger) loggersMap.get(targetLogger);
				selectedLogger.setLevel(Level.toLevel(targetLogLevel));
			}
			String loggerName = null;
			String loggerEffectiveLevel = null;
			String loggerParent = null;
			if (logger != null) {
				loggerName = logger.getName();
				loggerEffectiveLevel = String.valueOf(logger.getEffectiveLevel());
				loggerParent = (logger.getParent() == null ? null : logger.getParent().getName());
				System.out.println(loggerName + ": " + loggerEffectiveLevel + " (parent: " + loggerParent + ")");
			}
		}

	}
}
