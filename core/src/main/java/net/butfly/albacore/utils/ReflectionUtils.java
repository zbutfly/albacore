package net.butfly.albacore.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.exception.BusinessException;
import net.butfly.albacore.exception.SystemException;

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
		Collection<URL> urls = ClasspathHelper.forPackage(packagePrefix);
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

	public static Object safeMethodInvoke(Method method, Object object, Object... args) throws BusinessException {
		boolean accessible = method.isAccessible();
		try {
			method.setAccessible(true);
			return Proxy.isProxyClass(object.getClass()) ? Proxy.getInvocationHandler(object).invoke(object, method, args)
					: method.invoke(object, args);
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

	public static Object safeFieldGet(Field field, Object owner) {
		boolean accessible = field.isAccessible();
		try {
			field.setAccessible(true);
			return field.get(owner);
		} catch (IllegalAccessException e) {
			throw new SystemException("", e);
		} catch (IllegalArgumentException e) {
			throw new SystemException("", e);
		} finally {
			field.setAccessible(accessible);
		}
	}

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

}
