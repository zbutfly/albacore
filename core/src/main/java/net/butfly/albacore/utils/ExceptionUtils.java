package net.butfly.albacore.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ExceptionUtils extends UtilsBase {
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

//	public static RuntimeException wrap(Throwable ex) {
//		ex = unwrap(ex);
//		if (ex instanceof RuntimeException) return (RuntimeException) ex;
//		return new RuntimeException(ex);
//	}

	public static Exception unwrap(Throwable ex) {
		return unwrap(ex, null);
	}

	public static Exception unwrap(Throwable ex, Map<Class<? extends Throwable>, Method> wrapperClasses) {
		if (wrapperClasses == null) wrapperClasses = new HashMap<Class<? extends Throwable>, Method>();
		try {
			wrapperClasses.put(RuntimeException.class, RuntimeException.class.getMethod("getCause"));
			wrapperClasses.put(ExecutionException.class, ExecutionException.class.getMethod("getCause"));
			wrapperClasses
					.put(InvocationTargetException.class, InvocationTargetException.class.getMethod("getTargetException"));
			wrapperClasses.put(UndeclaredThrowableException.class,
					UndeclaredThrowableException.class.getMethod("getUndeclaredThrowable"));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		Throwable original;
		do {
			original = ex;
			ex = unwrapOneTime(ex, wrapperClasses);
		} while (!ex.equals(original));
		return (ex instanceof Exception) ? (Exception) ex : new RuntimeException(ex);
	}

	private static Throwable unwrapOneTime(Throwable ex, Map<Class<? extends Throwable>, Method> wrapperClasses) {
		for (Map.Entry<Class<? extends Throwable>, Method> m : wrapperClasses.entrySet())
			if (m.getKey().equals(ex.getClass())) try {
				Throwable th = (Throwable) m.getValue().invoke(ex);
				if (th != null) return th;
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
}
