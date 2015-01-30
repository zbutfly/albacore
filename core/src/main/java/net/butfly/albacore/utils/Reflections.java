package net.butfly.albacore.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.support.Bean;
import net.butfly.albacore.utils.imports.meta.MetaObject;

import org.reflections.Configuration;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.Scanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Reflections extends UtilsBase {
	private static final Logger logger = LoggerFactory.getLogger(Reflections.class);
	private static String DEFAULT_PACKAGE_PREFIX = "";
	private static Map<String, org.reflections.Reflections> reflections = new HashMap<String, org.reflections.Reflections>();

	private static org.reflections.Reflections reflections(String... packagePrefix) {
		if (null == packagePrefix || packagePrefix.length == 0) return reflections(DEFAULT_PACKAGE_PREFIX);
		// TODO
		else return reflections(packagePrefix[0]);
	}

	private static org.reflections.Reflections reflections(String packagePrefix) {
		if (null == packagePrefix) packagePrefix = DEFAULT_PACKAGE_PREFIX;
		org.reflections.Reflections r = reflections.get(packagePrefix);
		if (null != r) return r;
		FilterBuilder filterBuilder = new FilterBuilder().includePackage(packagePrefix);
		Collection<URL> urls = ClasspathHelper.forClassLoader();
		Scanner methodScanner = new MethodAnnotationsScanner().filterResultsBy(filterBuilder);
		Scanner subTypesScanner = new SubTypesScanner(false);
		Configuration configuration = new ConfigurationBuilder().filterInputsBy(filterBuilder).setUrls(urls)
				.addScanners(methodScanner, subTypesScanner);
		r = new org.reflections.Reflections(configuration);
		reflections.put(packagePrefix, r);
		return r;
	}

	@SuppressWarnings("unchecked")
	public static <T> T invoke(Method method, Object... targetAndParams) throws Exception {
		boolean accessible = method.isAccessible();
		try {
			method.setAccessible(true);
			if (Modifier.isStatic(method.getModifiers())) {
				if (targetAndParams == null || targetAndParams.length == 0) return (T) method.invoke(null);
				else return (T) method.invoke(null, targetAndParams);
			} else {
				Objects.notNull(targetAndParams);
				if (targetAndParams.length != method.getParameterCount() + 1) throw new NullPointerException(
						"Non-static mathod invoking need 1 target object, and " + method.getParameterCount() + " arguments");
				else {
					if (Proxy.isProxyClass(targetAndParams[0].getClass())) return (T) Proxy.getInvocationHandler(
							targetAndParams[0])
							.invoke(targetAndParams[0],
									method,
									targetAndParams.length == 1 ? null : Arrays.copyOfRange(targetAndParams, 1,
											targetAndParams.length));
					else {
						if (targetAndParams.length == 1) return (T) method.invoke(targetAndParams[0]);
						else return (T) method.invoke(targetAndParams[0],
								Arrays.copyOfRange(targetAndParams, 1, targetAndParams.length));
					}
				}
			}
		} catch (Throwable e) {
			throw Exceptions.unwrap(e);
		} finally {
			method.setAccessible(accessible);
		}
	}

	public static <T> T invoke(String methodName, Object targetOrClass, ParameterInfo... params) throws Exception {
		Objects.notEmpty(targetOrClass);
		Class<?>[] paramClasses = parameterClasses(params);
		Object[] paramValues = parameterValues(params);
		Method method;
		if (Class.class.equals(targetOrClass.getClass())) {// invoke static method
			method = findMethod(((Class<?>) targetOrClass), methodName, paramClasses);
			if (!Modifier.isStatic(method.getModifiers()))
				throw new IllegalArgumentException("Non-static mathod invoking need Object target , not Class target.");
		} else method = findMethod(targetOrClass.getClass(), methodName, paramClasses);
		method.setAccessible(true);
		boolean accessible = method.isAccessible();
		try {
			return invoke(method, paramValues);
		} catch (Throwable e) {
			throw Exceptions.unwrap(e);
		} finally {
			method.setAccessible(accessible);
		}
	}

	public static Method findMethod(Class<?> targetClass, String methodName, Class<?>... parameterTypes) {
		while (targetClass != null) {
			try {
				return targetClass.getDeclaredMethod(methodName, parameterTypes);
			} catch (NoSuchMethodException e) {} catch (SecurityException e) {}
			targetClass = targetClass.getSuperclass();
		}
		return null;
	}

	public static class MethodInfo extends Bean<MethodInfo> {
		private static final long serialVersionUID = 7736704702258827973L;
		private Class<?>[] parametersClasses;
		private Class<?> returnClass;

		public MethodInfo(Class<?>[] parametersClasses, Class<?> returnClass) {
			super();
			this.parametersClasses = parametersClasses;
			this.returnClass = returnClass;
		}

		public Class<?>[] parametersClasses() {
			return parametersClasses;
		}

		public Class<?> returnClass() {
			return returnClass;
		}
	}

	public static final class ParameterInfo extends Bean<ParameterInfo> {
		private static final long serialVersionUID = -8834764434029866955L;
		private Class<?> parameterClass;
		private Object parameterValue;

		private ParameterInfo(Class<?> parameterClass, Object parameterValue) {
			super();
			this.parameterClass = parameterClass;
			this.parameterValue = parameterValue;
		}

		public Class<?> parameterClass() {
			return parameterClass;
		}

		public Object parameterValue() {
			return parameterValue;
		}
	}

	public static ParameterInfo parameter(Object parameterValue, Class<?> parameterClass) {
		return new ParameterInfo(parameterClass, parameterValue);
	}

	public static ParameterInfo parameter(Object parameterValue) {
		if (null == parameterValue) throw new NullPointerException();
		return new ParameterInfo(parameterValue.getClass(), parameterValue);
	}

	public static Class<?>[] parameterClasses(ParameterInfo... paramInfo) {
		if (null == paramInfo || paramInfo.length == 0) return new Class<?>[0];
		Class<?>[] r = new Class<?>[paramInfo.length];
		for (int i = 0; i < paramInfo.length; i++)
			r[i] = paramInfo[i].parameterClass;
		return r;
	}

	public static Object[] parameterValues(ParameterInfo... paramInfo) {
		if (null == paramInfo || paramInfo.length == 0) return new Object[0];
		Object[] r = new Object[paramInfo.length];
		for (int i = 0; i < paramInfo.length; i++)
			r[i] = paramInfo[i].parameterValue;
		return r;
	}

	public static <T> T construct(Class<T> clazz, ParameterInfo... paramInfo) {
		if (null == paramInfo || paramInfo.length == 0) try {
			return clazz.newInstance();
		} catch (Exception ex) {
			logger.error("Construction failure: default constructor error", Exceptions.unwrap(ex));
			return null;
		}
		Constructor<T> constructor;
		try {
			constructor = clazz.getDeclaredConstructor(parameterClasses(paramInfo));
		} catch (Exception ex) {
			logger.error("Construction failure: constructor not defined", Exceptions.unwrap(ex));
			return null;
		}
		boolean accessible = constructor.isAccessible();
		try {
			constructor.setAccessible(true);
			return constructor.newInstance(parameterValues(paramInfo));
		} catch (Exception ex) {
			logger.error("Construction failure: error on constructing", Exceptions.unwrap(ex));
			return null;
		} finally {
			constructor.setAccessible(accessible);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> Class<T> forClassName(String className) {
		try {
			return (Class<T>) Class.forName(className);
		} catch (Throwable e) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> T construct(String className, ParameterInfo... paramInfo) {
		return (T) construct(forClassName(className), paramInfo);
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
