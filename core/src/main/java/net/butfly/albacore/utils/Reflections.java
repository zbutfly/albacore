package net.butfly.albacore.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.github.classgraph.ClassGraph;
import io.github.classgraph.ClassInfo;
import io.github.classgraph.ClassInfoList;
import io.github.classgraph.ScanResult;
import net.butfly.albacore.Albacore;
import net.butfly.albacore.support.Values;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

public final class Reflections extends Utils {
	private static final Logger logger = Logger.getLogger(Reflections.class);

	public static String packageName(Class<?> cl) {
		Package pkg = cl.getPackage();
		return null == pkg ? "" : cl.getPackage().getName();
	}

	public static boolean anyEmpty(Object... value) {
		for (Object v : value) {
			if (null == v) return true;
			if (v.getClass().isArray() && Array.getLength(v) == 0) return true;
			if (v instanceof CharSequence && ((CharSequence) v).length() == 0) return true;
		}
		return false;
	}

	public static void noneNull(String msg, Object... value) {
		for (Object v : value) if (null == v) throw new IllegalArgumentException(msg);
	}

	public static boolean anyNull(Object... value) {
		for (Object v : value) if (null == v) return true;
		return false;
	}

	public static <T> T construct(final Class<T> cls, Object... parameters) {
		final Class<?> parameterTypes[] = Refs.toClass(parameters);
		return construct(cls, parameters, parameterTypes);
	}

	public static <T> T construct(final Class<T> cls, Object[] args, Class<?>[] parameterTypes) {
		final Constructor<T> ctor = Values.getMatchingConstructors(cls, parameterTypes);
		if (ctor == null) {
			logger.warn("No such constructor on object: " + cls.getName());
			return null;
		}
		if (!Refs.accessible(ctor)) ctor.setAccessible(true);
		try {
			return ctor.newInstance(args);
		} catch (Exception e) {
			logger.error("Construction failure", Exceptions.unwrap(e));
			return null;
		}
	}

	public static <T> T construct(String className, Object... parameters) {
		return construct(forClassName(className), parameters);
	}

	@SuppressWarnings("unchecked")
	public static <T> T invoke(Object targetOrClass, String methodName, Object... parameters) throws NoSuchMethodException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Objects.notEmpty(targetOrClass);
		if (null == parameters) parameters = new Object[0];
		final Class<?>[] parameterTypes = Refs.toClass(parameters);
		boolean isStatic = Class.class.equals(targetOrClass.getClass());
		final Class<?> targetClass = isStatic ? (Class<?>) targetOrClass : targetOrClass.getClass();
		final Method method = Values.getMatchingAccessibleMethod(targetClass, methodName, parameterTypes);
		if (method == null) throw new NoSuchMethodException("No such accessible method: " + methodName + "() on object: " + targetClass
				.getName());
		return (T) method.invoke(isStatic ? null : targetOrClass, parameters);
	}

	public static void set(Object owner, Field field, Object value) {
		boolean accessible = Refs.accessible(field);
		try {
			field.setAccessible(true);
			field.set(owner, value);
		} catch (Throwable e) {
			throw Exceptions.wrap(e);
		} finally {
			field.setAccessible(accessible);
		}
	}

	public static <T> void set(Object obj, String field, T value) {
		set(obj, Reflections.getDeclaredField(obj.getClass(), field), value);
	}

	@SuppressWarnings("unchecked")
	public static <T> T get(Object obj, Field field) {
		boolean flag = Refs.accessible(field);
		if (!flag) field.setAccessible(true);
		try {
			return (T) field.get(obj);
		} catch (IllegalArgumentException | IllegalAccessException e) {
			throw new RuntimeException(e);
		} finally {
			field.setAccessible(flag);
		}
	}

	public static <T> T get(Object obj, String field) {
		return Class.class.isAssignableFrom(obj.getClass()) ? get(null, getDeclaredField((Class<?>) obj, field))
				: get(obj, getDeclaredField(obj.getClass(), field));
	}

	public static void copy(Object src, Object dst) {
		for (Field f : getDeclaredFields(src.getClass())) {
			Field f1 = getDeclaredField(dst.getClass(), f.getName());
			if (null != f1) try {
				f1.setAccessible(true);
				f.setAccessible(true);
				f1.set(dst, f.get(src));
			} catch (IllegalArgumentException | IllegalAccessException e) {}
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> forClassName(String className) {
		try {
			return (Class<T>) Thread.currentThread().getContextClassLoader().loadClass(className);
		} catch (Exception e) {
			return null;
		}
	}

	private static final Map<String, ScanResult> CGS = Maps.of();
	private static final int CG_WORKER_NUM = Integer.parseInt(System.getProperty(Albacore.Props.PROP_CLASS_SCAN_WORKERS_NUM, "0"));

	private static ScanResult cpscaner(String... pkgs) {
		Arrays.sort(pkgs);
		return CGS.computeIfAbsent(String.join(",", pkgs), k -> {
			long s = System.currentTimeMillis();
			logger.info("Classpath scan on [" + k + "] begining....");
			ScanResult r = null;
			try {
				ClassGraph cg = new ClassGraph();
				// if (Systems.isDebug()) cg = cg.verbose();
				cg = cg.enableAllInfo();// Scan classes, methods, fields, annotations
				if (pkgs.length > 0) cg = cg.whitelistPackages(pkgs); // Scan com.xyz and subpackages (omit to scan all packages)
				r = CG_WORKER_NUM > 0 ? cg.scan(CG_WORKER_NUM) : cg.scan();
			} finally {
				logger.info("Classpath scan on [" + k + "] finished in (" + (System.currentTimeMillis() - s) + ") ms with [" + (null == r ? 0
						: r.getClasspathURIs().size()) + "] entries.");
			}
			if (null != r) r.getClasspathURIs().forEach(u -> logger.debug("Classpath entry: " + u));
			return r;
		});

	}

	@SuppressWarnings("unchecked")
	public static <T> Set<Class<? extends T>> getSubClasses(Class<T> parentClass, String... packagePrefix) {
		if (Modifier.isFinal(parentClass.getModifiers())) return new HashSet<>();
		final Set<Class<? extends T>> r = new HashSet<>();
		// try (ScanResult sr = cpscaner(packagePrefix)) {
		ClassInfo pci = cpscaner(packagePrefix).getClassInfo(parentClass.getName());
		ClassInfoList cli = pci.isInterface() ? pci.getClassesImplementing() : pci.getSubclasses();
		for (ClassInfo ci : cli) r.add((Class<? extends T>) ci.loadClass());
		// }
		return r;
	}

	public static Set<Class<?>> getClassesAnnotatedWith(Class<? extends Annotation> annotation, String... packagePrefix) {
		Set<Class<?>> r = new HashSet<>();
		// try (ScanResult sr = cpscaner(packagePrefix)) {
		cpscaner(packagePrefix).getClassInfo(annotation.getName()).getClassesWithAnnotation().forEach(ci1 -> r.add(ci1.loadClass()));
		// }
		return r;
	}

	public static Field getDeclaredField(Class<?> clazz, String name) {
		noneNull("", clazz, name);
		while (null != clazz) try {
			return clazz.getDeclaredField(name);
		} catch (NoSuchFieldException ex) {
			clazz = clazz.getSuperclass();
		}
		return null;
	}

	public static Field[] getDeclaredFields(Class<?> clazz) {
		noneNull("", clazz);
		Map<String, Field> fields = new HashMap<String, Field>();
		while (clazz != null) {
			for (Field field : clazz.getDeclaredFields()) {
				if (fields.containsKey(field.getName())) continue;
				int mod = field.getModifiers();
				if (Modifier.isFinal(mod) || Modifier.isStatic(mod) || Modifier.isTransient(mod) || Modifier.isVolatile(mod)) continue;
				fields.put(field.getName(), field);
			}
			clazz = clazz.getSuperclass();
		}
		return fields.values().toArray(new Field[fields.size()]);
	}

	public static Field[] getDeclaredFieldsAnnotatedWith(Class<?> clazz, Class<? extends Annotation> annotation) {
		return Instances.fetch(() -> {
			Set<Field> s = new HashSet<Field>();
			for (Field f : getDeclaredFields(clazz)) if (f.isAnnotationPresent(annotation)) s.add(f);
			return s.toArray(new Field[s.size()]);
		}, Field[].class, clazz, annotation);
	}

	public static Method getDeclaredMethod(Class<?> clazz, String name, Class<?>... paramTypes) {
		while (null != clazz) {
			try {
				return clazz.getDeclaredMethod(name, paramTypes);
			} catch (NoSuchMethodException ex) {}
			clazz = clazz.getSuperclass();
		}
		return null;
	}

	public static Method[] getDeclaredMethods(Class<?> clazz) {
		Collection<Method> found = new ArrayList<Method>();
		while (clazz != null) {
			for (Method m1 : clazz.getDeclaredMethods()) {
				boolean overridden = false;

				for (Method m2 : found) {
					if (m2.getName().equals(m1.getName()) && Arrays.deepEquals(m1.getParameterTypes(), m2.getParameterTypes())) {
						overridden = true;
						break;
					}
				}

				if (!overridden) found.add(m1);
			}

			clazz = clazz.getSuperclass();
		}

		return found.toArray(new Method[found.size()]);
	}

	public static boolean isAny(Class<?> cl, Class<?>... target) {
		for (Class<?> t : target) if (t.isAssignableFrom(cl)) return true;
		return false;
	}

	public static boolean isAny(Object v, Class<?>... target) {
		Class<?> cl = null == v ? Void.class : v.getClass();
		return isAny(cl, target);
	}

	public <T> T unwrapProxy(T object) {
		if (null == object) return null;
		if (!Proxy.isProxyClass(object.getClass())) return object;
		return get(object, "h");
	}

	@SuppressWarnings("unchecked")
	public static <D, S extends D> S wrap(D from, Class<S> to) {
		return to.isAssignableFrom(from.getClass()) ? (S) from : Reflections.construct(to, from);
	}
}
