package net.butfly.albacore.utils;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.LoggerFactory;

public class Exceptions {
	static final org.slf4j.Logger logger = LoggerFactory.getLogger(Exceptions.class);

	public interface Code {
		// code for system exceptions.
		String ENCRYPT_CODE = "SYS_500";
		String REFLEC_CODE = "SYS_400";
		String DATA_CODE = "SYS_300";

		// code for business exceptions.
		String AUTH_CODE = "BIZ_100";
		String VALID_CODE = "BIZ_200";
	}

	public static String getStackTrace(Throwable e) {
		// to avoid JDK BUG, u can not invoke e.printStackTrace directly.
		StringWriter s = new StringWriter();
		PrintWriter w = new PrintWriter(s);
		w.println(e);
		StackTraceElement[] trace = e.getStackTrace();
		for (int i = 0; i < trace.length; i++)
			w.println("\tat " + trace[i]);
		Throwable cause = e.getCause();
		if (cause != null && cause != e) {
			w.println("Caused by " + cause);
			w.println(cause);
		}
		return s.toString();
	}

	public static String getStackTrace() {
		StackTraceElement[] s = Thread.currentThread().getStackTrace();
		if (null == s || s.length < 2) return null;
		return s[1].toString();
	}

	public static Throwable[] unlink(Throwable th) {
		List<Throwable> list = new ArrayList<Throwable>();
		for (Throwable t = th; t != null && !list.contains(t); t = t.getCause())
			list.add(t);
		return list.toArray(new Throwable[list.size()]);
	}

	public static RuntimeException wrap(Throwable ex) {
		return wrap(ex, RuntimeException.class);
	}

	@SuppressWarnings("unchecked")
	public static <T extends Exception> T wrap(Throwable ex, Class<T> expect) {
		ex = unwrap(ex);
		if (expect.isAssignableFrom(ex.getClass())) return (T) ex;
		try {
			return expect.getConstructor(Throwable.class).newInstance(ex);
		} catch (Exception e) {
			throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T extends Exception> T wrap(Throwable ex, Class<T> expect, String message) {
		if (null == message) return wrap(ex, expect);
		ex = unwrap(ex);
		if (expect.isAssignableFrom(ex.getClass())) return (T) ex;
		try {
			return expect.getConstructor(String.class, Throwable.class).newInstance(message, ex);
		} catch (Exception e) {
			throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
		}
	}

	private static final ReentrantReadWriteLock METHODS_LOCK = new ReentrantReadWriteLock();
	private static final Map<Class<? extends Throwable>, Method> WRAPPING_METHODS = initWrappingMethods();

	private static Map<Class<? extends Throwable>, Method> initWrappingMethods() {
		try {
			Map<Class<? extends Throwable>, Method> m = new ConcurrentHashMap<>();
			m.put(ExecutionException.class, ExecutionException.class.getMethod("getCause"));
			m.put(InvocationTargetException.class, InvocationTargetException.class.getMethod("getTargetException"));
			m.put(UndeclaredThrowableException.class, UndeclaredThrowableException.class.getMethod("getUndeclaredThrowable"));
			m.put(RuntimeException.class, RuntimeException.class.getMethod("getCause"));
			return m;
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

	public static void unwrap(Class<? extends Throwable> t, String methodName) {
		METHODS_LOCK.writeLock().lock();
		try {
			WRAPPING_METHODS.put(t, t.getMethod(methodName));
		} catch (NoSuchMethodException e) {
			throw new RuntimeException();
		} finally {
			METHODS_LOCK.writeLock().unlock();
		}
	}

	public static Throwable unwrap(Throwable ex) {
		if (null == ex) return null;
		METHODS_LOCK.readLock().lock();
		try {
			for (Entry<Class<? extends Throwable>, Method> t : WRAPPING_METHODS.entrySet())
				if (t.getKey().isAssignableFrom(ex.getClass())) try {
					Throwable cause = (Throwable) t.getValue().invoke(ex);
					return null == cause || ex.equals(cause) ? ex : unwrap(cause);
				} catch (Exception e) {}
		} finally {
			METHODS_LOCK.readLock().unlock();
		}
		return ex;
	}

	/**
	 * used by meta only
	 * 
	 * @param wrapped
	 * @return
	 */
	public static Throwable unwrapThrowable(Throwable wrapped) {
		Throwable unwrapped = wrapped;
		while (true) {
			if (unwrapped instanceof InvocationTargetException) {
				unwrapped = ((InvocationTargetException) unwrapped).getTargetException();
			} else if (unwrapped instanceof UndeclaredThrowableException) {
				unwrapped = ((UndeclaredThrowableException) unwrapped).getUndeclaredThrowable();
			} else {
				return unwrapped;
			}
		}
	}

	public static void printStackTrace(Throwable t, String code, PrintStream s) {
		s.print(null == code || "".equals(code) ? "" : "[" + code + "] ");
		t.printStackTrace(s);
	}

	public static interface ExceptAsDefault<T> {
		T get() throws Exception;
	}

	public static <T> T def(ExceptAsDefault<T> getting, T def) {
		try {
			return getting.get();
		} catch (Exception e) {
			logger.trace("Value fetch failure, use default value: [" + e.getMessage() + "].");
			return def;
		}
	}
}
