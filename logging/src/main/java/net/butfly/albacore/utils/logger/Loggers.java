package net.butfly.albacore.utils.logger;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Supplier;

import org.slf4j.event.Level;

public interface Loggers {
	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE, ElementType.PACKAGE })
	@interface Logging {
		Level value()

		default Level.INFO;

		Level debugLevel()

		default Level.DEBUG;

		Level hiLevel()

		default Level.ERROR; // not implemented

		boolean force() default true;
	}

	static String shrinkClassname(String origin) {
		if (origin.endsWith(".")) origin = origin.substring(0, origin.length() - 1);
		if (origin.length() == 0) origin = Logger.class.toString() + "$UnknownSource";
		return origin;
	}

	static Logging ing(Class<?> c) {
		Logging ing;
		while (null == (ing = c.getAnnotation(Logging.class)) && c.isMemberClass()) c = c.getDeclaringClass();
		return ing;
	}

	@SuppressWarnings("deprecation")
	static Logging ing(ClassLoader cl, Package pkg) {
		Logging ing = null;
		while (null != pkg) {
			if (null != (ing = pkg.getAnnotation(Logging.class))) return ing;
			String n = pkg.getName();
			int p = n.lastIndexOf('.');
			if (p < 0) return null;
			String parent = n.substring(0, p);
			if (parent.length() == 0) return null;
			pkg = Package.getPackage(parent);
		}
		return null;
	}

	static Set<Supplier<Map<String, Object>>> mdcs = new ConcurrentSkipListSet<>();

	static void mdcing(Supplier<Map<String, Object>> mdcing) {
		mdcs.add(mdcing);
	}
}
