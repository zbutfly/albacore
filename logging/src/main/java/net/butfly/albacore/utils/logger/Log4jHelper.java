package net.butfly.albacore.utils.logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.slf4j.impl.Log4jLoggerAdapter;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

interface Log4jHelper {
	static void checkConf() {
		URL res = Thread.currentThread().getContextClassLoader().getResource("log4j.properties");
		if (null != res) System.err.println("log4j.properties found in CLASSPATH [" + res.toString()
				+ "], but is not recommended. log4j.xml is recommended.");
		res = Thread.currentThread().getContextClassLoader().getResource("log4j.xml");
		if (null == res) {
			res = Thread.currentThread().getContextClassLoader().getResource("net/butfly/albacore/utils/logger/log4j-default.xml");
			if (null == res) throw new IllegalArgumentException("default log4j.xml not found.");
			// org.apache.log4j.xml.DOMConfigurator.configure(load(res));
			System.setProperty("log4j.configuration", res.toString());
		}
		if (res.getPath().indexOf(".jar!") > 0) //
			System.err.println("log4j configuration found in jar, dangerous and not recommended: \n" + res.getPath());
	}

	static void fixMDC() {
		Class<?> c;
		try {
			c = Class.forName("org.apache.log4j.helpers.Loader");
		} catch (ClassNotFoundException e) {
			return;
		}
		try {
			Field f = c.getDeclaredField("java1");
			f.setAccessible(true);
			if (f.getBoolean(null)) f.set(null, false);
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
			throw new IllegalAccessError();
		}
	}

	static Element load(URL res) {
		try (InputStream is = res.openStream(); Reader r = new InputStreamReader(is)) {
			return DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(new InputSource(r)).getDocumentElement();
		} catch (IOException | SAXException | ParserConfigurationException e) {
			throw new IllegalArgumentException(e);
		}
	}

	static org.apache.log4j.Logger log4j(Log4jLoggerAdapter slf) {
		try {
			Field f = Log4jLoggerAdapter.class.getDeclaredField("logger");
			f.setAccessible(true);
			return (org.apache.log4j.Logger) f.get(slf);
		} catch (NoSuchFieldException | SecurityException | IllegalAccessException e) {
			throw new IllegalArgumentException(e);
		}
	}

	static org.slf4j.Logger changeLayout(Log4jLoggerAdapter slf, org.slf4j.event.Level level) {
		log4j(slf).getAllAppenders();
		log4j(slf).setLevel(net.butfly.albacore.utils.logger.Logger.LEVELS_SLF_TO_LOG4J.get(level));
		return slf;
	}

	static org.slf4j.Logger changeLevel(Log4jLoggerAdapter slf, org.slf4j.event.Level level) {
		log4j(slf).setLevel(net.butfly.albacore.utils.logger.Logger.LEVELS_SLF_TO_LOG4J.get(level));
		return slf;
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

	@SuppressWarnings("rawtypes")
	static void flushAll() {
		Set<FileAppender> flushedFileAppenders = new HashSet<FileAppender>();
		Enumeration currentLoggers = LogManager.getLoggerRepository().getCurrentLoggers();
		Object nextLogger;
		Enumeration allAppenders;
		while (currentLoggers.hasMoreElements()) if ((nextLogger = currentLoggers.nextElement()) instanceof Logger) {
			Logger currentLogger = (Logger) nextLogger;
			allAppenders = currentLogger.getAllAppenders();
			while (allAppenders.hasMoreElements()) {
				Object nextElement = allAppenders.nextElement();
				if (nextElement instanceof FileAppender) {
					FileAppender fileAppender = (FileAppender) nextElement;
					net.butfly.albacore.utils.logger.Logger.normalLogger.info("Logger[log4j] flushing : " + fileAppender.getName());
					if (!flushedFileAppenders.contains(fileAppender) && !fileAppender.getImmediateFlush()) {
						flushedFileAppenders.add(fileAppender);
						fileAppender.setImmediateFlush(true);
						currentLogger.info(null);
					} else {}
				}
			}
		}
	}
}
