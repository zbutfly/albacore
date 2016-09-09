package net.butfly.albacore.utils;

import java.lang.annotation.Annotation;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import io.github.lukehutch.fastclasspathscanner.FastClasspathScanner;
import net.butfly.albacore.utils.imports.meta.MetaObject;

public final class Reflections extends Utils {
	private static final Logger logger = LoggerFactory.getLogger(Reflections.class);

	@SuppressWarnings("unchecked")
	public static <T> Class<T> forClassName(String className) {
		try {
			return (Class<T>) Thread.currentThread().getContextClassLoader().loadClass(className);
		} catch (Exception e) {
			return null;
		}
	}

	public static Field getDeclaredField(Class<?> clazz, String name) {
		while (null != clazz) {
			try {
				return clazz.getDeclaredField(name);
			} catch (NoSuchFieldException ex) {}
			clazz = clazz.getSuperclass();
		}
		return null;
	}

	public static Field[] getDeclaredFields(Class<?> clazz) {
		Map<String, Field> fields = new HashMap<String, Field>();
		while (clazz != null) {
			for (Field field : clazz.getDeclaredFields()) {
				if (!fields.containsKey(field.getName())) {
					fields.put(field.getName(), field);
				}
			}

			clazz = clazz.getSuperclass();
		}

		return fields.values().toArray(new Field[fields.size()]);
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

	@SuppressWarnings("unchecked")
	public static <T> T invoke(Object targetOrClass, String methodName, Object... parameters) throws Exception {
		Objects.notEmpty(targetOrClass);
		if (null == parameters) parameters = new Object[0];
		final Class<?>[] parameterTypes = toClass(parameters);
		boolean isStatic = Class.class.equals(targetOrClass.getClass());
		final Class<?> targetClass = isStatic ? (Class<?>) targetOrClass : targetOrClass.getClass();
		final Method method = getMatchingAccessibleMethod(targetClass, methodName, parameterTypes);
		if (method == null) { throw new NoSuchMethodException("No such accessible method: " + methodName + "() on object: " + targetClass
				.getName()); }
		return (T) method.invoke(isStatic ? null : targetOrClass, parameters);
	}

	public static <T> T construct(String className, Object... parameters) {
		Class<T> clazz = forClassName(className);
		return construct(clazz, parameters);
	}

	public static <T> T construct(final Class<T> cls, Object... parameters) {
		final Class<?> parameterTypes[] = toClass(parameters);
		return construct(cls, parameters, parameterTypes);
	}

	public static <T> T construct(final Class<T> cls, Object[] args, Class<?>[] parameterTypes) {
		final Constructor<T> ctor = getMatchingConstructors(cls, parameterTypes);
		if (ctor == null) {
			logger.error("No such constructor on object: " + cls.getName());
			return null;
		}
		if (!ctor.isAccessible()) ctor.setAccessible(true);
		try {
			return ctor.newInstance(args);
		} catch (Exception e) {
			logger.error("Construction failure", Exceptions.unwrap(e));
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> Constructor<T> getMatchingConstructors(final Class<T> cls, final Class<?>... parameterTypes) {
		if (cls == null) return null;
		try {
			return cls.getDeclaredConstructor(parameterTypes);
		} catch (final NoSuchMethodException e) {}
		Constructor<T> result = null;
		for (Constructor<?> ctor : cls.getDeclaredConstructors())
			if (isAssignable(parameterTypes, ctor.getParameterTypes(), true) && ctor != null) {
				setAccessibleWorkaround(ctor);
				if (result == null || compareParameterTypes(ctor.getParameterTypes(), result.getParameterTypes(), parameterTypes) < 0) {
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

	private static final Joiner j = Joiner.on(";");

	public static <T> Set<Class<? extends T>> getSubClasses(Class<T> parentClass, String... packagePrefix) {
		return Instances.fetch(() -> {
			final Set<Class<? extends T>> r = new HashSet<>();
			new FastClasspathScanner(packagePrefix).matchSubclassesOf(parentClass, c -> r.add(c)).scan();
			return r;
		}, parentClass, j.join(packagePrefix));
	}

	public static Class<?>[] getClassesAnnotatedWith(Class<? extends Annotation> annotation, String... packagePrefix) {
		return Instances.fetch(() -> {
			Set<Class<?>> r = new HashSet<>();
			new FastClasspathScanner(packagePrefix).matchClassesWithAnnotation(annotation, c -> r.add(c)).scan();
			return r.toArray(new Class[r.size()]);
		}, annotation, j.join(packagePrefix));
	}

	public static Field[] getDeclaredFieldsAnnotatedWith(Class<?> clazz, Class<? extends Annotation> annotation) {
		return Instances.fetch(() -> {
			Set<Field> s = new HashSet<Field>();
			for (Field f : getDeclaredFields(clazz))
				if (f.isAnnotationPresent(annotation)) s.add(f);
			return s.toArray(new Field[s.size()]);
		}, clazz, annotation);
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

	/**
	 * Maps primitive {@code Class}es to their corresponding wrapper
	 * {@code Class}.
	 */
	private static final Map<Class<?>, Class<?>> primitiveWrapperMap = new HashMap<Class<?>, Class<?>>();

	static {
		primitiveWrapperMap.put(Boolean.TYPE, Boolean.class);
		primitiveWrapperMap.put(Byte.TYPE, Byte.class);
		primitiveWrapperMap.put(Character.TYPE, Character.class);
		primitiveWrapperMap.put(Short.TYPE, Short.class);
		primitiveWrapperMap.put(Integer.TYPE, Integer.class);
		primitiveWrapperMap.put(Long.TYPE, Long.class);
		primitiveWrapperMap.put(Double.TYPE, Double.class);
		primitiveWrapperMap.put(Float.TYPE, Float.class);
		primitiveWrapperMap.put(Void.TYPE, Void.TYPE);
	}

	/**
	 * Maps wrapper {@code Class}es to their corresponding primitive types.
	 */
	private static final Map<Class<?>, Class<?>> wrapperPrimitiveMap = new HashMap<Class<?>, Class<?>>();

	static {
		for (final Class<?> primitiveClass : primitiveWrapperMap.keySet()) {
			final Class<?> wrapperClass = primitiveWrapperMap.get(primitiveClass);
			if (!primitiveClass.equals(wrapperClass)) {
				wrapperPrimitiveMap.put(wrapperClass, primitiveClass);
			}
		}
	}

	/**
	 * <p>
	 * Converts the specified primitive Class object to its corresponding
	 * wrapper Class object.
	 * </p>
	 *
	 * <p>
	 * NOTE: From v2.2, this method handles {@code Void.TYPE}, returning
	 * {@code Void.TYPE}.
	 * </p>
	 *
	 * @param cls
	 *            the class to convert, may be null
	 * @return the wrapper class for {@code cls} or {@code cls} if {@code cls}
	 *         is not a primitive. {@code null} if null input.
	 * @since 2.1
	 */
	public static Class<?> primitiveToWrapper(final Class<?> cls) {
		Class<?> convertedClass = cls;
		if (cls != null && cls.isPrimitive()) {
			convertedClass = primitiveWrapperMap.get(cls);
		}
		return convertedClass;
	}

	/**
	 * <p>
	 * Converts the specified wrapper class to its corresponding primitive
	 * class.
	 * </p>
	 *
	 * <p>
	 * This method is the counter part of {@code primitiveToWrapper()}. If the
	 * passed in class is a wrapper class for a primitive type, this primitive
	 * type will be returned (e.g. {@code Integer.TYPE} for
	 * {@code Integer.class}). For other classes, or if the parameter is
	 * <b>null</b>, the return value is <b>null</b>.
	 * </p>
	 *
	 * @param cls
	 *            the class to convert, may be <b>null</b>
	 * @return the corresponding primitive type if {@code cls} is a wrapper
	 *         class, <b>null</b> otherwise
	 * @see #primitiveToWrapper(Class)
	 * @since 2.4
	 */
	public static Class<?> wrapperToPrimitive(final Class<?> cls) {
		return wrapperPrimitiveMap.get(cls);
	}

	/**
	 * <p>
	 * Finds an accessible method that matches the given name and has compatible
	 * parameters. Compatible parameters mean that every method parameter is
	 * assignable from the given parameters. In other words, it finds a method
	 * with the given name that will take the parameters given.
	 * </p>
	 *
	 * <p>
	 * This method is used by
	 * {@link #invokeMethod(Object object, String methodName, Object[] args, Class[] parameterTypes)}
	 * .
	 * </p>
	 *
	 * <p>
	 * This method can match primitive parameter by passing in wrapper classes.
	 * For example, a {@code Boolean} will match a primitive {@code boolean}
	 * parameter.
	 * </p>
	 *
	 * @param cls
	 *            find method in this class
	 * @param methodName
	 *            find method with this name
	 * @param parameterTypes
	 *            find method with most compatible parameters
	 * @return The accessible method
	 */
	public static Method getMatchingAccessibleMethod(final Class<?> cls, final String methodName, final Class<?>... parameterTypes) {
		try {
			final Method method = cls.getMethod(methodName, parameterTypes);
			setAccessibleWorkaround(method);
			return method;
		} catch (final NoSuchMethodException e) { // NOPMD - Swallow the
													// exception
		}
		// search through all methods
		Method bestMatch = null;
		final Method[] methods = cls.getMethods();
		for (final Method method : methods) {
			// compare name and parameters
			if (method.getName().equals(methodName) && isAssignable(parameterTypes, method.getParameterTypes(), true)) {
				// get accessible version of method
				final Method accessibleMethod = getAccessibleMethod(method);
				if (accessibleMethod != null && (bestMatch == null || compareParameterTypes(accessibleMethod.getParameterTypes(), bestMatch
						.getParameterTypes(), parameterTypes) < 0)) {
					bestMatch = accessibleMethod;
				}
			}
		}
		if (bestMatch != null) setAccessibleWorkaround(bestMatch);
		return bestMatch;
	}

	/**
	 * <p>
	 * Checks if one {@code Class} can be assigned to a variable of another
	 * {@code Class}.
	 * </p>
	 *
	 * <p>
	 * Unlike the {@link Class#isAssignableFrom(java.lang.Class)} method, this
	 * method takes into account widenings of primitive classes and {@code null}
	 * s.
	 * </p>
	 *
	 * <p>
	 * Primitive widenings allow an int to be assigned to a long, float or
	 * double. This method returns the correct result for these cases.
	 * </p>
	 *
	 * <p>
	 * {@code Null} may be assigned to any reference type. This method will
	 * return {@code true} if {@code null} is passed in and the toClass is
	 * non-primitive.
	 * </p>
	 *
	 * <p>
	 * Specifically, this method tests whether the type represented by the
	 * specified {@code Class} parameter can be converted to the type
	 * represented by this {@code Class} object via an identity conversion
	 * widening primitive or widening reference conversion. See
	 * <em><a href="http://docs.oracle.com/javase/specs/">The Java Language
	 * Specification</a></em> , sections 5.1.1, 5.1.2 and 5.1.4 for details.
	 * </p>
	 *
	 * <p>
	 * <strong>Since Lang 3.0,</strong> this method will default behavior for
	 * calculating assignability between primitive and wrapper types
	 * <em>corresponding to the running Java version</em>; i.e. autoboxing will
	 * be the default behavior in VMs running Java versions &gt; 1.5.
	 * </p>
	 *
	 * @param cls
	 *            the Class to check, may be null
	 * @param toClass
	 *            the Class to try to assign into, returns false if null
	 * @return {@code true} if assignment possible
	 */
	public static boolean isAssignable(final Class<?> cls, final Class<?> toClass) {
		return isAssignable(cls, toClass, true/*
												 * SystemUtils.
												 * isJavaVersionAtLeast(
												 * JavaVersion.JAVA_1_5)
												 */);
	}

	/**
	 * <p>
	 * Checks if one {@code Class} can be assigned to a variable of another
	 * {@code Class}.
	 * </p>
	 *
	 * <p>
	 * Unlike the {@link Class#isAssignableFrom(java.lang.Class)} method, this
	 * method takes into account widenings of primitive classes and {@code null}
	 * s.
	 * </p>
	 *
	 * <p>
	 * Primitive widenings allow an int to be assigned to a long, float or
	 * double. This method returns the correct result for these cases.
	 * </p>
	 *
	 * <p>
	 * {@code Null} may be assigned to any reference type. This method will
	 * return {@code true} if {@code null} is passed in and the toClass is
	 * non-primitive.
	 * </p>
	 *
	 * <p>
	 * Specifically, this method tests whether the type represented by the
	 * specified {@code Class} parameter can be converted to the type
	 * represented by this {@code Class} object via an identity conversion
	 * widening primitive or widening reference conversion. See
	 * <em><a href="http://docs.oracle.com/javase/specs/">The Java Language
	 * Specification</a></em> , sections 5.1.1, 5.1.2 and 5.1.4 for details.
	 * </p>
	 *
	 * @param cls
	 *            the Class to check, may be null
	 * @param toClass
	 *            the Class to try to assign into, returns false if null
	 * @param autoboxing
	 *            whether to use implicit autoboxing/unboxing between primitives
	 *            and wrappers
	 * @return {@code true} if assignment possible
	 */
	public static boolean isAssignable(Class<?> cls, final Class<?> toClass, final boolean autoboxing) {
		if (toClass == null) { return false; }
		// have to check for null, as isAssignableFrom doesn't
		if (cls == null) { return !toClass.isPrimitive(); }
		// autoboxing:
		if (autoboxing) {
			if (cls.isPrimitive() && !toClass.isPrimitive()) {
				cls = primitiveToWrapper(cls);
				if (cls == null) { return false; }
			}
			if (toClass.isPrimitive() && !cls.isPrimitive()) {
				cls = wrapperToPrimitive(cls);
				if (cls == null) { return false; }
			}
		}
		if (cls.equals(toClass)) { return true; }
		if (cls.isPrimitive()) {
			if (toClass.isPrimitive() == false) { return false; }
			if (Integer.TYPE.equals(cls)) { return Long.TYPE.equals(toClass) || Float.TYPE.equals(toClass) || Double.TYPE.equals(toClass); }
			if (Long.TYPE.equals(cls)) { return Float.TYPE.equals(toClass) || Double.TYPE.equals(toClass); }
			if (Boolean.TYPE.equals(cls)) { return false; }
			if (Double.TYPE.equals(cls)) { return false; }
			if (Float.TYPE.equals(cls)) { return Double.TYPE.equals(toClass); }
			if (Character.TYPE.equals(cls)) { return Integer.TYPE.equals(toClass) || Long.TYPE.equals(toClass) || Float.TYPE.equals(toClass)
					|| Double.TYPE.equals(toClass); }
			if (Short.TYPE.equals(cls)) { return Integer.TYPE.equals(toClass) || Long.TYPE.equals(toClass) || Float.TYPE.equals(toClass)
					|| Double.TYPE.equals(toClass); }
			if (Byte.TYPE.equals(cls)) { return Short.TYPE.equals(toClass) || Integer.TYPE.equals(toClass) || Long.TYPE.equals(toClass)
					|| Float.TYPE.equals(toClass) || Double.TYPE.equals(toClass); }
			// should never get here
			return false;
		}
		return toClass.isAssignableFrom(cls);
	}

	/**
	 * <p>
	 * Checks if an array of Classes can be assigned to another array of
	 * Classes.
	 * </p>
	 *
	 * <p>
	 * This method calls {@link #isAssignable(Class, Class) isAssignable} for
	 * each Class pair in the input arrays. It can be used to check if a set of
	 * arguments (the first parameter) are suitably compatible with a set of
	 * method parameter types (the second parameter).
	 * </p>
	 *
	 * <p>
	 * Unlike the {@link Class#isAssignableFrom(java.lang.Class)} method, this
	 * method takes into account widenings of primitive classes and {@code null}
	 * s.
	 * </p>
	 *
	 * <p>
	 * Primitive widenings allow an int to be assigned to a {@code long},
	 * {@code float} or {@code double}. This method returns the correct result
	 * for these cases.
	 * </p>
	 *
	 * <p>
	 * {@code Null} may be assigned to any reference type. This method will
	 * return {@code true} if {@code null} is passed in and the toClass is
	 * non-primitive.
	 * </p>
	 *
	 * <p>
	 * Specifically, this method tests whether the type represented by the
	 * specified {@code Class} parameter can be converted to the type
	 * represented by this {@code Class} object via an identity conversion
	 * widening primitive or widening reference conversion. See
	 * <em><a href="http://docs.oracle.com/javase/specs/">The Java Language
	 * Specification</a></em> , sections 5.1.1, 5.1.2 and 5.1.4 for details.
	 * </p>
	 *
	 * @param classArray
	 *            the array of Classes to check, may be {@code null}
	 * @param toClassArray
	 *            the array of Classes to try to assign into, may be
	 *            {@code null}
	 * @param autoboxing
	 *            whether to use implicit autoboxing/unboxing between primitives
	 *            and wrappers
	 * @return {@code true} if assignment possible
	 */
	public static boolean isAssignable(Class<?>[] classArray, Class<?>[] toClassArray, final boolean autoboxing) {
		if (isSameLength(classArray, toClassArray) == false) { return false; }
		if (classArray == null) {
			classArray = new Class[0];
		}
		if (toClassArray == null) {
			toClassArray = new Class[0];
		}
		for (int i = 0; i < classArray.length; i++) {
			if (isAssignable(classArray[i], toClassArray[i], autoboxing) == false) { return false; }
		}
		return true;
	}

	/**
	 * <p>
	 * Converts an array of {@code Object} in to an array of {@code Class}
	 * objects. If any of these objects is null, a null element will be inserted
	 * into the array.
	 * </p>
	 *
	 * <p>
	 * This method returns {@code null} for a {@code null} input array.
	 * </p>
	 *
	 * @param array
	 *            an {@code Object} array
	 * @return a {@code Class} array, {@code null} if null array input
	 * @since 2.4
	 */
	public static Class<?>[] toClass(final Object... array) {
		if (array == null) return null;
		else if (array.length == 0) return new Class[0];
		final Class<?>[] classes = new Class[array.length];
		for (int i = 0; i < array.length; i++)
			classes[i] = array[i] == null ? null : array[i].getClass();
		return classes;
	}

	/**
	 * <p>
	 * Checks whether two arrays are the same length, treating {@code null}
	 * arrays as length {@code 0}.
	 *
	 * <p>
	 * Any multi-dimensional aspects of the arrays are ignored.
	 * </p>
	 *
	 * @param array1
	 *            the first array, may be {@code null}
	 * @param array2
	 *            the second array, may be {@code null}
	 * @return {@code true} if length of arrays matches, treating {@code null}
	 *         as an empty array
	 */
	public static boolean isSameLength(final Object[] array1, final Object[] array2) {
		if ((array1 == null && array2 != null && array2.length > 0) || (array2 == null && array1 != null && array1.length > 0)
				|| (array1 != null && array2 != null && array1.length != array2.length)) { return false; }
		return true;
	}

	/**
	 * Returns whether a {@link Member} is accessible.
	 * 
	 * @param m
	 *            Member to check
	 * @return {@code true} if <code>m</code> is accessible
	 */
	static boolean isAccessible(final Member m) {
		return m != null && Modifier.isPublic(m.getModifiers()) && !m.isSynthetic();
	}

	/**
	 * <p>
	 * Returns an accessible method (that is, one that can be invoked via
	 * reflection) that implements the specified Method. If no such method can
	 * be found, return {@code null}.
	 * </p>
	 *
	 * @param method
	 *            The method that we wish to call
	 * @return The accessible method
	 */
	public static Method getAccessibleMethod(Method method) {
		if (!isAccessible(method)) { return null; }
		// If the declaring class is public, we are done
		final Class<?> cls = method.getDeclaringClass();
		if (Modifier.isPublic(cls.getModifiers())) { return method; }
		final String methodName = method.getName();
		final Class<?>[] parameterTypes = method.getParameterTypes();

		// Check the implemented interfaces and subinterfaces
		method = getAccessibleMethodFromInterfaceNest(cls, methodName, parameterTypes);

		// Check the superclass chain
		if (method == null) {
			method = getAccessibleMethodFromSuperclass(cls, methodName, parameterTypes);
		}
		return method;
	}

	/**
	 * <p>
	 * Returns an accessible method (that is, one that can be invoked via
	 * reflection) that implements the specified method, by scanning through all
	 * implemented interfaces and subinterfaces. If no such method can be found,
	 * return {@code null}.
	 * </p>
	 *
	 * <p>
	 * There isn't any good reason why this method must be {@code private}. It
	 * is because there doesn't seem any reason why other classes should call
	 * this rather than the higher level methods.
	 * </p>
	 *
	 * @param cls
	 *            Parent class for the interfaces to be checked
	 * @param methodName
	 *            Method name of the method we wish to call
	 * @param parameterTypes
	 *            The parameter type signatures
	 * @return the accessible method or {@code null} if not found
	 */
	private static Method getAccessibleMethodFromInterfaceNest(Class<?> cls, final String methodName, final Class<?>... parameterTypes) {
		// Search up the superclass chain
		for (; cls != null; cls = cls.getSuperclass()) {

			// Check the implemented interfaces of the parent class
			final Class<?>[] interfaces = cls.getInterfaces();
			for (int i = 0; i < interfaces.length; i++) {
				// Is this interface public?
				if (!Modifier.isPublic(interfaces[i].getModifiers())) {
					continue;
				}
				// Does the method exist on this interface?
				try {
					return interfaces[i].getDeclaredMethod(methodName, parameterTypes);
				} catch (final NoSuchMethodException e) { // NOPMD
					/*
					 * Swallow, if no method is found after the loop then this
					 * method returns null.
					 */
				}
				// Recursively check our parent interfaces
				Method method = getAccessibleMethodFromInterfaceNest(interfaces[i], methodName, parameterTypes);
				if (method != null) { return method; }
			}
		}
		return null;
	}

	/**
	 * <p>
	 * Returns an accessible method (that is, one that can be invoked via
	 * reflection) by scanning through the superclasses. If no such method can
	 * be found, return {@code null}.
	 * </p>
	 *
	 * @param cls
	 *            Class to be checked
	 * @param methodName
	 *            Method name of the method we wish to call
	 * @param parameterTypes
	 *            The parameter type signatures
	 * @return the accessible method or {@code null} if not found
	 */
	private static Method getAccessibleMethodFromSuperclass(final Class<?> cls, final String methodName, final Class<?>... parameterTypes) {
		Class<?> parentClass = cls.getSuperclass();
		while (parentClass != null) {
			if (Modifier.isPublic(parentClass.getModifiers())) {
				try {
					return parentClass.getMethod(methodName, parameterTypes);
				} catch (final NoSuchMethodException e) {
					return null;
				}
			}
			parentClass = parentClass.getSuperclass();
		}
		return null;
	}

	private static final int ACCESS_TEST = Modifier.PUBLIC | Modifier.PROTECTED | Modifier.PRIVATE;

	/**
	 * XXX Default access superclass workaround.
	 *
	 * When a {@code public} class has a default access superclass with
	 * {@code public} members, these members are accessible. Calling them from
	 * compiled code works fine. Unfortunately, on some JVMs, using reflection
	 * to invoke these members seems to (wrongly) prevent access even when the
	 * modifier is {@code public}. Calling {@code setAccessible(true)} solves
	 * the problem but will only work from sufficiently privileged code. Better
	 * workarounds would be gratefully accepted.
	 * 
	 * @param o
	 *            the AccessibleObject to set as accessible
	 * @return a boolean indicating whether the accessibility of the object was
	 *         set to true.
	 */
	static boolean setAccessibleWorkaround(final AccessibleObject o) {
		if (o == null || o.isAccessible()) return false;
		final Member m = (Member) o;
		if (Modifier.isPublic(m.getModifiers()) && (m.getDeclaringClass().getModifiers() & ACCESS_TEST) == 0) {
			try {
				o.setAccessible(true);
				return true;
			} catch (final SecurityException e) { // NOPMD
				// ignore in favor of subsequent IllegalAccessException
			}
		}
		return false;
	}

	/**
	 * Compares the relative fitness of two sets of parameter types in terms of
	 * matching a third set of runtime parameter types, such that a list ordered
	 * by the results of the comparison would return the best match first
	 * (least).
	 *
	 * @param left
	 *            the "left" parameter set
	 * @param right
	 *            the "right" parameter set
	 * @param actual
	 *            the runtime parameter types to match against {@code left}/
	 *            {@code right}
	 * @return int consistent with {@code compare} semantics
	 */
	static int compareParameterTypes(final Class<?>[] left, final Class<?>[] right, final Class<?>[] actual) {
		final float leftCost = getTotalTransformationCost(actual, left);
		final float rightCost = getTotalTransformationCost(actual, right);
		return leftCost < rightCost ? -1 : rightCost < leftCost ? 1 : 0;
	}

	/**
	 * Returns the sum of the object transformation cost for each class in the
	 * source argument list.
	 * 
	 * @param srcArgs
	 *            The source arguments
	 * @param destArgs
	 *            The destination arguments
	 * @return The total transformation cost
	 */
	private static float getTotalTransformationCost(final Class<?>[] srcArgs, final Class<?>[] destArgs) {
		float totalCost = 0.0f;
		for (int i = 0; i < srcArgs.length; i++) {
			Class<?> srcClass, destClass;
			srcClass = srcArgs[i];
			destClass = destArgs[i];
			totalCost += getObjectTransformationCost(srcClass, destClass);
		}
		return totalCost;
	}

	/**
	 * Gets the number of steps required needed to turn the source class into
	 * the destination class. This represents the number of steps in the object
	 * hierarchy graph.
	 * 
	 * @param srcClass
	 *            The source class
	 * @param destClass
	 *            The destination class
	 * @return The cost of transforming an object
	 */
	private static float getObjectTransformationCost(Class<?> srcClass, final Class<?> destClass) {
		if (destClass.isPrimitive()) { return getPrimitivePromotionCost(srcClass, destClass); }
		float cost = 0.0f;
		while (srcClass != null && !destClass.equals(srcClass)) {
			if (destClass.isInterface() && isAssignable(srcClass, destClass)) {
				// slight penalty for interface match.
				// we still want an exact match to override an interface match,
				// but
				// an interface match should override anything where we have to
				// get a superclass.
				cost += 0.25f;
				break;
			}
			cost++;
			srcClass = srcClass.getSuperclass();
		}
		/*
		 * If the destination class is null, we've travelled all the way up to
		 * an Object match. We'll penalize this by adding 1.5 to the cost.
		 */
		if (srcClass == null) {
			cost += 1.5f;
		}
		return cost;
	}

	/** Array of primitive number types ordered by "promotability" */
	private static final Class<?>[] ORDERED_PRIMITIVE_TYPES = { Byte.TYPE, Short.TYPE, Character.TYPE, Integer.TYPE, Long.TYPE, Float.TYPE,
			Double.TYPE };

	/**
	 * Gets the number of steps required to promote a primitive number to
	 * another type.
	 * 
	 * @param srcClass
	 *            the (primitive) source class
	 * @param destClass
	 *            the (primitive) destination class
	 * @return The cost of promoting the primitive
	 */
	private static float getPrimitivePromotionCost(final Class<?> srcClass, final Class<?> destClass) {
		float cost = 0.0f;
		Class<?> cls = srcClass;
		if (!cls.isPrimitive()) {
			// slight unwrapping penalty
			cost += 0.1f;
			cls = wrapperToPrimitive(cls);
		}
		for (int i = 0; cls != destClass && i < ORDERED_PRIMITIVE_TYPES.length; i++) {
			if (cls == ORDERED_PRIMITIVE_TYPES[i]) {
				cost += 0.1f;
				if (i < ORDERED_PRIMITIVE_TYPES.length - 1) {
					cls = ORDERED_PRIMITIVE_TYPES[i + 1];
				}
			}
		}
		return cost;
	}

	/**
	 * Holds a map of commonly used interface types (mostly collections) to a
	 * class that implements the interface and will, by default, be instantiated
	 * when an instance of the interface is needed.
	 */
	protected static final Map<Class<?>, Class<?>> interfaceImplementations = new HashMap<Class<?>, Class<?>>();

	static {
		interfaceImplementations.put(Collection.class, ArrayList.class);
		interfaceImplementations.put(List.class, ArrayList.class);
		interfaceImplementations.put(Set.class, HashSet.class);
		interfaceImplementations.put(SortedSet.class, TreeSet.class);
		interfaceImplementations.put(Queue.class, LinkedList.class);
		interfaceImplementations.put(Map.class, HashMap.class);
		interfaceImplementations.put(SortedMap.class, TreeMap.class);
	}

	/**
	 * Attempts to determine an implementing class for the interface provided
	 * and instantiate it using a default constructor.
	 *
	 * @param interfaceType
	 *            an interface (or abstract class) to make an instance of
	 * @return an instance of the interface type supplied
	 * @throws InstantiationException
	 *             if no implementation type has been configured
	 * @throws IllegalAccessException
	 *             if thrown by the JVM during class instantiation
	 */
	@SuppressWarnings("unchecked")
	public static <T> T constructInterface(Class<T> interfaceType) {
		if (!Modifier.isInterface(interfaceType.getModifiers())) throw new IllegalArgumentException(
				"Only interfaces have default implementation.");
		Class<?> impl = interfaceImplementations.get(interfaceType);
		if (impl != null) try {
			return (T) impl.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			return null;
		}
		throw new IllegalArgumentException("Stripes needed to instantiate a property who's declared type as an "
				+ "interface (which obviously cannot be instantiated. The interface is not "
				+ "one that Stripes is aware of, so no implementing class was known. The " + "interface type was: '" + interfaceType
						.getName() + "'. To fix this " + "you'll need to do one of three things. 1) Change the getter/setter methods "
				+ "to use a concrete type so that Stripes can instantiate it. 2) in the bean's "
				+ "setContext() method pre-instantiate the property so Stripes doesn't have to. "
				+ "3) Bug the Stripes author ;)  If the interface is a JDK type it can easily be "
				+ "fixed. If not, if enough people ask, a generic way to handle the problem " + "might get implemented.");

	}

	/**
	 * The set of method that annotation classes inherit, and should be avoided
	 * when toString()ing an annotation class.
	 */
	private static final Set<String> INHERITED_ANNOTATION_METHODS = new HashSet<>(Arrays.asList("toString", "equals", "hashCode",
			"annotationType", "getClass"));

	/**
	 * <p>
	 * A better (more concise) toString method for annotation types that yields
	 * a String that should look more like the actual usage of the annotation in
	 * a class. The String produced is similar to that produced by calling
	 * toString() on the annotation directly, with the following differences:
	 * </p>
	 *
	 * <ul>
	 * <li>Uses the classes simple name instead of it's fully qualified name.
	 * </li>
	 * <li>Only outputs attributes that are set to non-default values.</li>
	 *
	 * <p>
	 * If, for some unforseen reason, an exception is thrown within this method
	 * it will be caught and the return value will be {@code ann.toString()}.
	 *
	 * @param ann
	 *            the annotation to convert to a human readable String
	 * @return a human readable String form of the annotation and it's
	 *         attributes
	 */
	public static String toString(Annotation ann) {
		try {
			Class<? extends Annotation> type = ann.annotationType();
			StringBuilder builder = new StringBuilder(128);
			builder.append("@");
			builder.append(type.getSimpleName());

			boolean appendedAnyParameters = false;
			Method[] methods = type.getMethods();
			for (Method method : methods) {
				if (!INHERITED_ANNOTATION_METHODS.contains(method.getName())) {
					Object defaultValue = method.getDefaultValue();
					Object actualValue = method.invoke(ann);

					// If we have arrays, they have to be treated a little
					// differently
					Object[] defaultArray = null, actualArray = null;
					if (Object[].class.isAssignableFrom(method.getReturnType())) {
						defaultArray = (Object[]) defaultValue;
						actualArray = (Object[]) actualValue;
					}

					// Only print an attribute if it isn't set to the default
					// value
					if ((defaultArray != null && !Arrays.equals(defaultArray, actualArray)) || (defaultArray == null && !actualValue.equals(
							defaultValue))) {

						if (appendedAnyParameters) {
							builder.append(", ");
						} else {
							builder.append("(");
						}

						builder.append(method.getName());
						builder.append("=");

						if (actualArray != null) {
							builder.append(Arrays.toString(actualArray));
						} else {
							builder.append(actualValue);
						}

						appendedAnyParameters = true;
					}
				}
			}

			if (appendedAnyParameters) {
				builder.append(")");
			}

			return builder.toString();
		} catch (Exception e) {
			return ann.toString();
		}
	}
}
