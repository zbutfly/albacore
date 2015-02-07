package net.butfly.albacore.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import net.butfly.albacore.utils.async.Task;
import net.butfly.albacore.utils.imports.meta.MetaObject;

import org.apache.commons.lang3.ClassUtils;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.reflections.Configuration;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.Scanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Reflections extends Utils {
	private static final Logger logger = LoggerFactory.getLogger(Reflections.class);
	private static String DEFAULT_PACKAGE_PREFIX = "";

	private static org.reflections.Reflections reflections(String... packagePrefix) {
		if (null == packagePrefix || packagePrefix.length == 0) return reflections(DEFAULT_PACKAGE_PREFIX);
		// TODO
		else return reflections(packagePrefix[0]);
	}

	private static org.reflections.Reflections reflections(final String packagePrefix) {
		return Instances.fetch(new Task.Callable<org.reflections.Reflections>() {
			@Override
			public org.reflections.Reflections call() {
				FilterBuilder filterBuilder = new FilterBuilder().includePackage(null == packagePrefix ? DEFAULT_PACKAGE_PREFIX
						: packagePrefix);
				Collection<URL> urls = ClasspathHelper.forClassLoader();
				Scanner methodScanner = new MethodAnnotationsScanner().filterResultsBy(filterBuilder);
				Scanner subTypesScanner = new SubTypesScanner(false);
				Configuration configuration = new ConfigurationBuilder().filterInputsBy(filterBuilder).setUrls(urls)
						.addScanners(methodScanner, subTypesScanner);
				return new org.reflections.Reflections(configuration);
			}
		}, packagePrefix);
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> forClassName(String className) {
		try {
			return (Class<T>) ClassUtils.getClass(className);
		} catch (ClassNotFoundException e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T invoke(Object targetOrClass, String methodName, Object... parameters) throws Exception {
		Objects.notEmpty(targetOrClass);
		T result = Class.class.equals(targetOrClass.getClass()) ? (T) MethodUtils.invokeStaticMethod((Class<?>) targetOrClass,
				methodName, parameters) : (T) MethodUtils.invokeMethod(targetOrClass, methodName, parameters);
		return result;
	}

	public static <T> T construct(String className, Object... parameters) {
		Class<T> clazz = forClassName(className);
		return construct(clazz, parameters);
	}

	public static <T> T construct(final Class<T> cls, Object... args) {
		final Class<?> parameterTypes[] = ClassUtils.toClass(args);
		return construct(cls, args, parameterTypes);
	}

	public static <T> T construct(final Class<T> cls, Object[] args, Class<?>[] parameterTypes) {
		final Constructor<T> ctor = getMatchingConstructor(cls, parameterTypes);
		if (ctor == null) {
			logger.error("No such constructor on object: " + cls.getName());
			return null;
		}
		try {
			return ctor.newInstance(args);
		} catch (Exception e) {
			logger.error("Construction failure" + Exceptions.unwrap(e));
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> Constructor<T> getMatchingConstructor(final Class<T> cls, final Class<?>... parameterTypes) {
		if (cls == null) return null;
		try {
			return cls.getDeclaredConstructor(parameterTypes);
		} catch (final NoSuchMethodException e) {}
		Constructor<T> result = null;
		for (Constructor<?> ctor : cls.getDeclaredConstructors())
			if (ClassUtils.isAssignable(parameterTypes, ctor.getParameterTypes(), true) && ctor != null) {
				MemberUtils.setAccessibleWorkaround(ctor);
				if (result == null
						|| MemberUtils.compareParameterTypes(ctor.getParameterTypes(), result.getParameterTypes(),
								parameterTypes) < 0) {
					result = (Constructor<T>) ctor;
				}
			}
		return result;
	}

	/**
	 * @param owner
	 *            instance for non-static field and class for static field.
	 * @param name
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T get(Object owner, String name) {
		if (null == owner) throw new NullPointerException();
		MetaObject meta = Objects.createMeta(owner);
		if (meta.hasGetter(name)) return (T) meta.getValue(name);
		else throw new RuntimeException("No getter method for target.");
	}

	/**
	 * set field by field instance directly.
	 */
	public static void set(Object owner, Field field, Object value) {
		boolean accessible = field.isAccessible();
		try {
			field.setAccessible(true);
			field.set(owner, value);
		} catch (Throwable e) {
			throw Exceptions.wrap(e);
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
	public static void set(Object owner, String name, Object value) {
		if (null == owner) throw new NullPointerException();

		MetaObject meta = Objects.createMeta(owner);
		if (meta.hasSetter(name)) meta.setValue(name, value);
		else throw new RuntimeException("No setter method for target.");
	}

	public static <T> Set<Class<? extends T>> getSubClasses(Class<T> parentClass, String... packagePrefix) {
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

	public static Class<?>[] getClassesAnnotatedWith(Class<? extends Annotation> annotation, String... packagePrefix) {
		return reflections(packagePrefix).getTypesAnnotatedWith(annotation).toArray(new Class[0]);
	}

	public static Field[] getFieldsAnnotatedWith(Class<? extends Annotation> annotation, String... packagePrefix) {
		Set<Field> s = reflections(packagePrefix).getFieldsAnnotatedWith(annotation);
		return s.toArray(new Field[s.size()]);
	}

	public static Method[] getMethodsAnnotatedWith(Class<? extends Annotation> annotation, String... packagePrefix) {
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
		return get(object, "h");
	}
}
