package net.butfly.albacore.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
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
import net.butfly.albacore.support.Bean;
import net.butfly.albacore.utils.imports.meta.MetaObject;

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
		} catch (Throwable e) {
			e = Exceptions.unwrap(e);
			if (e instanceof BusinessException) throw (BusinessException) e;
			else throw new RuntimeException(e);
		} finally {
			method.setAccessible(accessible);
		}
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

	public static ParameterInfo parameters(Class<?> parameterClass, Object parameterValue) {
		return new ParameterInfo(parameterClass, parameterValue);
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

	public static <T> T safeConstruct(Class<T> clazz, ParameterInfo... paramInfo) {
		if (null == paramInfo || paramInfo.length == 0) try {
			return clazz.newInstance();
		} catch (Exception ex) {
			ex = Exceptions.unwrap(ex);
			throw new RuntimeException(ex);
		}
		Constructor<T> constructor;
		try {
			constructor = clazz.getDeclaredConstructor(parameterClasses(paramInfo));
		} catch (Exception ex) {
			ex = Exceptions.unwrap(ex);
			throw new RuntimeException(ex);
		}
		boolean accessible = constructor.isAccessible();
		try {
			constructor.setAccessible(true);
			return constructor.newInstance(parameterValues(paramInfo));
		} catch (Exception ex) {
			ex = Exceptions.unwrap(ex);
			throw new RuntimeException(ex);
		} finally {
			constructor.setAccessible(accessible);
		}
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
		MetaObject meta = ObjectUtils.createMeta(owner);
		if (meta.hasGetter(name)) return (T) meta.getValue(name);
		else throw new RuntimeException("No getter method for target.");
	}

	/**
	 * set field by field instance directly.
	 */
	public static void safeFieldSet(Field field, Object owner, Object value) {
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
	public static void safeFieldSet(Object owner, String name, Object value) {
		if (null == owner) throw new NullPointerException();

		MetaObject meta = ObjectUtils.createMeta(owner);
		if (meta.hasSetter(name)) meta.setValue(name, value);
		else throw new RuntimeException("No setter method for target.");
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
