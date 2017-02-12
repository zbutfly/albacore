package net.butfly.albacore.utils;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Exceptions extends Utils {
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
		return Reflections.construct(expect, ex);
	}

	@SuppressWarnings("unchecked")
	public static <T extends Exception> T wrap(Throwable ex, Class<T> expect, String message) {
		if (null == message) return wrap(ex, expect);
		ex = unwrap(ex);
		if (expect.isAssignableFrom(ex.getClass())) return (T) ex;
		return Reflections.construct(expect, message, ex);
	}

	private static final List<Method> WRAPPING_METHODS;
	static {
		try {
			WRAPPING_METHODS = new ArrayList<>(Arrays.asList(//
					ExecutionException.class.getMethod("getCause"), //
					InvocationTargetException.class.getMethod("getTargetException"), //
					UndeclaredThrowableException.class.getMethod("getUndeclaredThrowable"), //
					RuntimeException.class.getMethod("getCause") //
			));
		} catch (NoSuchMethodException e) {
			throw new RuntimeException(e);
		}
	}

	public static Throwable unwrap(Throwable ex) {
		return unwrap(ex, new Method[0]);
	}

	public static Throwable unwrap(Throwable ex, Method... otherMathods) {
		if (null == ex) return null;
		if (null != otherMathods && otherMathods.length > 0) WRAPPING_METHODS.addAll(Arrays.asList(otherMathods));
		for (Method m : WRAPPING_METHODS)
			if (m.getDeclaringClass().isAssignableFrom(ex.getClass())) try {
				Throwable cause = (Throwable) m.invoke(ex);
				return null == cause || ex.equals(cause) ? ex : unwrap(cause, otherMathods);
			} catch (Exception e) {}
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
}
