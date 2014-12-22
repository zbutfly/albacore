package net.butfly.albacore.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.exception.BusinessException;
import net.butfly.albacore.exception.SystemException;

import org.apache.commons.lang3.ClassUtils;
import org.reflections.Configuration;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.Scanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

public final class ReflectionUtils extends UtilsBase {
	private static String DEFAULT_PACKAGE_PREFIX = "";
	private static Map<String, Reflections> reflections = new HashMap<String, Reflections>();
	static {
		reflections(DEFAULT_PACKAGE_PREFIX);
	}

	private static Reflections reflections(String packagePrefix) {
		if (null == packagePrefix) packagePrefix = DEFAULT_PACKAGE_PREFIX;
		Reflections r = reflections.get(packagePrefix);
		if (null != r) return r;
		FilterBuilder filterBuilder = new FilterBuilder().includePackage(packagePrefix);
		Collection<URL> urls = ClasspathHelper.forClassLoader();
		Scanner methodScanner = new MethodAnnotationsScanner().filterResultsBy(filterBuilder);
		Scanner subTypesScanner = new SubTypesScanner(false);
		Configuration configuration = new ConfigurationBuilder().filterInputsBy(filterBuilder).setUrls(urls)
				.addScanners(methodScanner, subTypesScanner);
		r = new Reflections(configuration);
		reflections.put(packagePrefix, r);
		return r;
	}

	public static Class<?>[] getAnnotatedTypes(String packagePrefix, Class<? extends Annotation> annotationClass) {
		return reflections(packagePrefix).getTypesAnnotatedWith(annotationClass).toArray(new Class[0]);
	}

	@SuppressWarnings("unchecked")
	public static <T> T safeMethodInvoke(Method method, Object object, Object... args) throws BusinessException {
		boolean accessible = method.isAccessible();
		try {
			method.setAccessible(true);
			return Proxy.isProxyClass(object.getClass()) ? (T) Proxy.getInvocationHandler(object).invoke(object, method, args)
					: (T) method.invoke(object, args);
		} catch (IllegalAccessException e) {
			throw new SystemException("", e);
		} catch (IllegalArgumentException e) {
			throw new SystemException("", e);
		} catch (InvocationTargetException e) {
			Class<? extends Throwable> causeClass = e.getCause().getClass();
			if (BusinessException.class.isAssignableFrom(causeClass)) throw BusinessException.class.cast(causeClass);
			else throw new SystemException("", e.getCause());
		} catch (Throwable e) {
			throw new SystemException("", e);
		} finally {
			method.setAccessible(accessible);
		}
	}

	public static <T> T safeConstruct(Constructor<T> constructor, Object... args) throws BusinessException {
		boolean accessible = constructor.isAccessible();
		try {
			constructor.setAccessible(true);
			return constructor.newInstance(args);
		} catch (IllegalAccessException e) {
			throw new SystemException("", e);
		} catch (IllegalArgumentException e) {
			throw new SystemException("", e);
		} catch (InvocationTargetException e) {
			Class<? extends Throwable> causeClass = e.getCause().getClass();
			if (BusinessException.class.isAssignableFrom(causeClass)) throw BusinessException.class.cast(causeClass);
			else throw new SystemException("", e.getCause());
		} catch (InstantiationException e) {
			throw new SystemException("", e);
		} finally {
			constructor.setAccessible(accessible);
		}
	}

	public static Field[] getAllFieldsDeeply(Class<?> clazz) {
		List<Field> fs = new ArrayList<Field>();
		fs.addAll(Arrays.asList(clazz.getDeclaredFields()));

		for (Class<?> cl : ClassUtils.getAllSuperclasses(clazz))
			fs.addAll(Arrays.asList(cl.getDeclaredFields()));
		for (Class<?> cl : ClassUtils.getAllSuperclasses(clazz))
			fs.addAll(Arrays.asList(cl.getDeclaredFields()));
		return fs.toArray(new Field[fs.size()]);
	}

	public static Field[] getFieldsDeeply(Class<?> clazz, String name, boolean staticOnly) {
		List<Field> fs = new ArrayList<Field>();
		try {
			Field f = clazz.getDeclaredField(name);
			if (!staticOnly || Modifier.isStatic(f.getModifiers())) fs.add(f);
		} catch (Exception e) {}
		for (Class<?> cl : ClassUtils.getAllSuperclasses(clazz))
			try {
				Field f = cl.getDeclaredField(name);
				if (!staticOnly || Modifier.isStatic(f.getModifiers())) fs.add(f);
			} catch (Exception e) {}
		for (Class<?> cl : ClassUtils.getAllSuperclasses(clazz))
			try {
				Field f = cl.getDeclaredField(name);
				if (!staticOnly || Modifier.isStatic(f.getModifiers())) fs.add(f);
			} catch (Exception e) {}
		return fs.toArray(new Field[fs.size()]);
	}

	/**
	 * @param owner
	 *            instance for non-static field and class for static field.
	 * @param name
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T safeFieldGet(Object owner, String name) {
		if (null == owner) throw new NullPointerException();
		boolean forClass = Class.class.isAssignableFrom(owner.getClass());
		Class<?> clazz = forClass ? (Class<?>) owner : owner.getClass();
		Field[] fs = getFieldsDeeply(clazz, name, forClass);
		if (fs.length == 0) throw new RuntimeException(new NoSuchFieldException());

		Field field = fs[0];
		boolean accessible = field.isAccessible();
		try {
			field.setAccessible(true);
			return (T) field.get(forClass ? null : owner);
		} catch (IllegalAccessException e) {
			throw new SystemException("", e);
		} finally {
			field.setAccessible(accessible);
		}
	}

	/**
	 * set field by field instance directly.
	 */
	public static void safeFieldSet(Field field, Object owner, Object value) {
		boolean accessible = field.isAccessible();
		try {
			field.setAccessible(true);
			field.set(owner, value);
		} catch (IllegalAccessException e) {
			throw new SystemException("", e);
		} catch (IllegalArgumentException e) {
			throw new SystemException("", e);
		} finally {
			field.setAccessible(accessible);
		}
	}

	/**
	 * set field by name.
	 * 
	 * @param owner
	 *            instance for non-static field and class for static field.
	 */
	public static void safeFieldSet(Object owner, String name, Object value) {
		if (null == owner) throw new NullPointerException();
		boolean forClass = Class.class.isAssignableFrom(owner.getClass());
		Class<?> clazz = forClass ? (Class<?>) owner : owner.getClass();
		Field[] fs = getFieldsDeeply(clazz, name, forClass);
		if (fs.length == 0) throw new RuntimeException(new NoSuchFieldException());

		for (Field field : fs)
			if (null == value || field.getType().isAssignableFrom(value.getClass()))
				safeFieldSet(field, forClass ? null : owner, value);
	}

	public static <T> Set<Class<? extends T>> getSubClasses(Class<T> parentClass, String packagePrefix) {
		return reflections(packagePrefix).getSubTypesOf(parentClass);
	}

	public static Field[] getDeclaredFields(Class<?> clazz) {
		// XXX: use map to check override fields by name.
		Set<Field> set = new HashSet<Field>();
		while (null != clazz) {
			set.addAll(Arrays.asList(clazz.getDeclaredFields()));
			clazz = clazz.getSuperclass();
		}
		return set.toArray(new Field[set.size()]);
	}

	public static Field[] getDeclaredFieldsAnnotatedWith(Class<?> clazz, Class<? extends Annotation> annotation) {
		Set<Field> s = new HashSet<Field>();
		for (Field f : getDeclaredFields(clazz))
			if (f.isAnnotationPresent(annotation)) s.add(f);
		return s.toArray(new Field[s.size()]);
	}

	public static Field[] getFieldsAnnotatedWith(String packagePrefix, Class<? extends Annotation> annotation) {
		Set<Field> s = reflections(packagePrefix).getFieldsAnnotatedWith(annotation);
		return s.toArray(new Field[s.size()]);
	}

	public static Method[] getMethodsAnnotatedWith(String packagePrefix, Class<? extends Annotation> annotation) {
		Set<Method> s = reflections(packagePrefix).getMethodsAnnotatedWith(annotation);
		return s.toArray(new Method[s.size()]);
	}

	public static Class<?> getMainClass() {
		try {
			return Class.forName(System.getProperty("sun.java.command"));
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public <T> T unwrapProxy(T object) {
		if (null == object) return null;
		if (!Proxy.isProxyClass(object.getClass())) return object;
		return safeFieldGet(object, "h");
	}
}
