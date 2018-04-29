package net.butfly.albacore.utils;

import java.lang.instrument.Instrumentation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class Syss {
	private static AtomicBoolean moduleErrorLogged = new AtomicBoolean(false);

	public static long sizeOf(Object obj) {
		try {
			return ((Long) Class.forName("jdk.nashorn.internal.ir.debug.ObjectSizeCalculator").getMethod("getObjectSize", Object.class)
					.invoke(null, obj)).longValue();
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException
				| ClassNotFoundException e) {
			if (!moduleErrorLogged.getAndSet(true)) System.err.print(
					"WARNING: sizeOf not work on java 9+, add jvm argument if you want: \n\t--add-opens java.base/java.util=jdk.scripting.nashorn --add-opens java.base/java.lang=jdk.scripting.nashorn");
			return 0;
		}
	}

	public static void main(String[] args) {
		Map<String, String> m = new HashMap<>();
		for (int i = 0; i < 10; i++)
			m.put(String.valueOf(Math.random()), String.valueOf(Math.random()));
		// --add-opens java.base/java.util=jdk.scripting.nashorn --add-opens java.base/java.lang=jdk.scripting.nashorn
		System.out.println("Map: " + m + ":\nSize of map: " + sizeOf(m) + " bytes.");
		System.out.println("Map: " + m + ":\nSize of map: " + sizeOf(m) + " bytes.");
		System.out.println("Map: " + m + ":\nSize of map: " + sizeOf(m) + " bytes.");
		System.out.println("Map: " + m + ":\nSize of map: " + sizeOf(m) + " bytes.");
	}

	/**
	 * Add the following to your MANIFEST.MF: <br>
	 * <code>Premain-Class: net.butfly.albacore.utils.Syss.InstruUtil</code>
	 */
	static class InstruUtil {
		private static Instrumentation instrumentation;

		public static void premain(String args, Instrumentation inst) {
			instrumentation = inst;
		}

		public static long sizeOf(Object obj) {
			try {
				return instrumentation.getObjectSize(obj);
			} catch (Exception ex) {
				return 0;
			}
		}
	}

	static class UnsafeUtil {
		public static final sun.misc.Unsafe UNSAFE;
		static {
			Object theUnsafe = null;
			Exception exception = null;
			try {
				Class<?> uc = Class.forName("sun.misc.Unsafe");
				Field f = uc.getDeclaredField("theUnsafe");
				f.setAccessible(true);
				theUnsafe = f.get(uc);
			} catch (Exception e) {
				exception = e;
			}
			UNSAFE = (sun.misc.Unsafe) theUnsafe;
			if (UNSAFE == null) throw new Error("Could not obtain access to sun.misc.Unsafe", exception);
		}

		private UnsafeUtil() {}

		private static final int NR_BITS = Integer.valueOf(System.getProperty("sun.arch.data.model"));
		private static final int BYTE = 8;
		private static final int WORD = NR_BITS / BYTE;
		private static final int MIN_SIZE = 16;

		public static int sizeOf(Class<?> src) {
			List<Field> instanceFields = new LinkedList<Field>();
			do {
				if (src == Object.class) return MIN_SIZE;
				for (Field f : src.getDeclaredFields()) {
					if ((f.getModifiers() & Modifier.STATIC) == 0) {
						instanceFields.add(f);
					}
				}
				src = src.getSuperclass();
			} while (instanceFields.isEmpty());
			//
			// Get the field with the maximum offset
			//
			long maxOffset = 0;
			for (Field f : instanceFields) {
				long offset = UnsafeUtil.UNSAFE.objectFieldOffset(f);
				if (offset > maxOffset) maxOffset = offset;
			}
			return (((int) maxOffset / WORD) + 1) * WORD;
		}
	}
}
