package net.butfly.albacore.support;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import com.google.common.primitives.Primitives;

import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.Refs;
import net.butfly.albacore.utils.Utils;

public final class Values extends Utils {
	public static final String NULL_STR = String.valueOf((Object) null);
	/** Array of primitive number types ordered by "promotability" */
	private static final Class<?>[] ORDERED_PRIMITIVE_TYPES = { Byte.TYPE, Short.TYPE, Character.TYPE, Integer.TYPE, Long.TYPE, Float.TYPE,
			Double.TYPE };
	private static final int ACCESS_TEST = Modifier.PUBLIC | Modifier.PROTECTED | Modifier.PRIVATE;

	public static Class<?> is(Class<?> type) {
		if (null == type) return Void.class;
		for (Class<?> cl : VALUE_TYPES)
			if (cl.isAssignableFrom(type)) return cl;
		return null;
	}

	public static Class<?> is(Object value) {
		if (null == value) return Void.class;
		return is(value.getClass());
	}

	public static Class<?> isN(Class<?> type) {
		if (null == type) return null;
		for (Class<?> cl : NUMBER_TYPES)
			if (cl.isAssignableFrom(type)) return cl;
		return null;
	}

	public static Class<?> isN(Object value) {
		if (null == value) return null;
		return isN(value.getClass());
	}

	public static boolean isVs(Object value) {
		if (null == value) return false;
		Class<?> type = value.getClass();
		if (type.isArray() && null != is(type.getComponentType())) return true;
		if (Iterable.class.isAssignableFrom(type)) {
			Iterator<?> it = ((Iterable<?>) value).iterator();
			while (it.hasNext())
				if (null == is(it.next())) return false;
			return true;
		}
		return false;
	}

	public static String toString(Object value) {
		if (null == is(value)) throw new RuntimeException("Not simple value: " + value);
		if (null == value) return NULL_STR;
		if (Date.class.isAssignableFrom(value.getClass())) return String.valueOf(((Date) value).getTime());
		return String.valueOf(value);
	}

	@SuppressWarnings("unchecked")
	public static <T> T fromString(String str, Class<T> type) {
		if (null == str || str.length() == 0 || NULL_STR.equals(str)) return (T) null;
		final Class<?> vt = is(type);
		if (null == vt) throw new RuntimeException("Not simple type: " + type);
		if (Number.class.isAssignableFrom(type)) return Reflections.construct(type, str);
		return (T) FROM_STRING_METHOD.get(vt).apply(str);
	}

	/**
	 * <p>
	 * Collections the specified wrapper class to its corresponding primitive class.
	 * </p>
	 *
	 * <p>
	 * This method is the counter part of {@code primitiveToWrapper()}. If the passed in class is a wrapper class for a primitive type, this
	 * primitive type will be returned (e.g. {@code Integer.TYPE} for {@code Integer.class}). For other classes, or if the parameter is
	 * <b>null</b>, the return value is <b>null</b>.
	 * </p>
	 *
	 * @param cls
	 *            the class to convert, may be <b>null</b>
	 * @return the corresponding primitive type if {@code cls} is a wrapper class, <b>null</b> otherwise
	 * @see #primitiveToWrapper(Class)
	 * @since 2.4
	 */
	public static Class<?> wrapperToPrimitive(final Class<?> cls) {
		try {
			return (Class<?>) cls.getField("TYPE").get(null);
		} catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
			return null;
		}
	}

	public static void main(String... args) {
		fromString("1234", int.class);
	}

	private static final Class<?>[] VALUE_TYPES = new Class[] { //
			/** String **/
			CharSequence.class, char.class, Character.class, //
			/** Number **/
			Number.class, int.class, byte.class, short.class, long.class, double.class, float.class,
			/** Bool **/
			boolean.class, Boolean.class, //
			/** Date **/
			Date.class, //
			/** null **/
			Void.class };

	private static Map<Class<?>, Function<String, Object>> FROM_STRING_METHOD = new HashMap<>();

	static {
		FROM_STRING_METHOD.put(Void.class, s -> NULL_STR);
		FROM_STRING_METHOD.put(CharSequence.class, s -> s);
		FROM_STRING_METHOD.put(Character.class, s -> s.toCharArray()[0]);
		FROM_STRING_METHOD.put(char.class, s -> s.toCharArray()[0]);

		FROM_STRING_METHOD.put(byte.class, s -> Byte.valueOf(s).byteValue());
		FROM_STRING_METHOD.put(short.class, s -> Short.valueOf(s).shortValue());
		FROM_STRING_METHOD.put(int.class, s -> Integer.valueOf(s).intValue());
		FROM_STRING_METHOD.put(long.class, s -> Long.valueOf(s).longValue());
		FROM_STRING_METHOD.put(double.class, s -> Double.valueOf(s).doubleValue());
		FROM_STRING_METHOD.put(float.class, s -> Float.valueOf(s).floatValue());

		FROM_STRING_METHOD.put(boolean.class, s -> Boolean.valueOf(s).booleanValue());
		FROM_STRING_METHOD.put(Boolean.class, s -> Boolean.valueOf(s));
		FROM_STRING_METHOD.put(Date.class, s -> new Date(Long.parseLong(s)));
	}
	private static final Class<?>[] NUMBER_TYPES = new Class[] { //
			Number.class, int.class, byte.class, short.class, long.class, double.class, float.class };

	public static int sizing(Object v) {
		if (null == v) return 0;
		if (v instanceof Iterable<?>) return sizing(((Iterable<?>) v).iterator());
		if (v instanceof Iterator<?>) {
			int s = 0;
			while (((Iterator<?>) v).hasNext())
				s += sizing(((Iterator<?>) v).next());
			return s;
		}
		Class<?> cl = v.getClass();
		if (Map.class.isAssignableFrom(cl)) return sizing(((Map<?, ?>) v).values());
		if (byte[].class.isAssignableFrom(cl)) return ((byte[]) v).length;
		int cc = sizingPrimitive(cl.getComponentType());
		if (cc >= 0) return cc;
		if (cl.isArray()) {
			cc = sizingPrimitive(cl.getComponentType());
			if (cc >= 0) return cc * Array.getLength(v);
			int l = 0;
			for (int i = 0; i < Array.getLength(v); i++)
				l += sizing(Array.get(v, i));
			return l;
		}
		if (CharSequence.class.isAssignableFrom(cl)) return ((CharSequence) v).toString().getBytes().length;
		return 0;
	}

	private static int sizingPrimitive(Class<?> cl) {
		if (cl.isPrimitive()) cl = Primitives.wrap(cl);
		try {
			Field f = cl.getField("BYTES");
			if (Modifier.isStatic(f.getModifiers()) && Modifier.isPublic(f.getModifiers()) && Modifier.isFinal(f.getModifiers())
					&& int.class.equals(f.getType())) return (int) f.get(null);
		} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {}
		return -1;
	}

	/**
	 * <p>
	 * Checks if one {@code Class} can be assigned to a variable of another {@code Class}.
	 * </p>
	 *
	 * <p>
	 * Unlike the {@link Class#isAssignableFrom(java.lang.Class)} method, this method takes into account widenings of primitive classes and
	 * {@code null} s.
	 * </p>
	 *
	 * <p>
	 * Primitive widenings allow an int to be assigned to a long, float or double. This method returns the correct result for these cases.
	 * </p>
	 *
	 * <p>
	 * {@code Null} may be assigned to any reference type. This method will return {@code true} if {@code null} is passed in and the toClass
	 * is non-primitive.
	 * </p>
	 *
	 * <p>
	 * Specifically, this method tests whether the type represented by the specified {@code Class} parameter can be converted to the type
	 * represented by this {@code Class} object via an identity conversion widening primitive or widening reference conversion. See
	 * <em><a href="http://docs.oracle.com/javase/specs/">The Java Language Specification</a></em> , sections 5.1.1, 5.1.2 and 5.1.4 for
	 * details.
	 * </p>
	 *
	 * @param cls
	 *            the Class to check, may be null
	 * @param toClass
	 *            the Class to try to assign into, returns false if null
	 * @param autoboxing
	 *            whether to use implicit autoboxing/unboxing between primitives and wrappers
	 * @return {@code true} if assignment possible
	 */
	private static boolean isAssignable(Class<?> cls, final Class<?> toClass, final boolean autoboxing) {
		if (toClass == null) return false;
		// have to check for null, as isAssignableFrom doesn't
		if (cls == null) return !toClass.isPrimitive();
		// autoboxing:
		if (autoboxing) {
			if (cls.isPrimitive() && !toClass.isPrimitive()) {
				cls = Primitives.wrap(cls);
				if (cls == null) return false;
			}
			if (toClass.isPrimitive() && !cls.isPrimitive()) {
				cls = Values.wrapperToPrimitive(cls);
				if (cls == null) return false;
			}
		}
		if (cls.equals(toClass)) return true;
		if (cls.isPrimitive()) {
			if (toClass.isPrimitive() == false) return false;
			if (Integer.TYPE.equals(cls)) return Long.TYPE.equals(toClass) || Float.TYPE.equals(toClass) || Double.TYPE.equals(toClass);
			if (Long.TYPE.equals(cls)) return Float.TYPE.equals(toClass) || Double.TYPE.equals(toClass);
			if (Boolean.TYPE.equals(cls)) return false;
			if (Double.TYPE.equals(cls)) return false;
			if (Float.TYPE.equals(cls)) return Double.TYPE.equals(toClass);
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
	 * Checks if an array of Classes can be assigned to another array of Classes.
	 * </p>
	 *
	 * <p>
	 * This method calls {@link #isAssignable(Class, Class) isAssignable} for each Class pair in the input arrays. It can be used to check
	 * if a set of arguments (the first parameter) are suitably compatible with a set of method parameter types (the second parameter).
	 * </p>
	 *
	 * <p>
	 * Unlike the {@link Class#isAssignableFrom(java.lang.Class)} method, this method takes into account widenings of primitive classes and
	 * {@code null} s.
	 * </p>
	 *
	 * <p>
	 * Primitive widenings allow an int to be assigned to a {@code long}, {@code float} or {@code double}. This method returns the correct
	 * result for these cases.
	 * </p>
	 *
	 * <p>
	 * {@code Null} may be assigned to any reference type. This method will return {@code true} if {@code null} is passed in and the toClass
	 * is non-primitive.
	 * </p>
	 *
	 * <p>
	 * Specifically, this method tests whether the type represented by the specified {@code Class} parameter can be converted to the type
	 * represented by this {@code Class} object via an identity conversion widening primitive or widening reference conversion. See
	 * <em><a href="http://docs.oracle.com/javase/specs/">The Java Language Specification</a></em> , sections 5.1.1, 5.1.2 and 5.1.4 for
	 * details.
	 * </p>
	 *
	 * @param classArray
	 *            the array of Classes to check, may be {@code null}
	 * @param toClassArray
	 *            the array of Classes to try to assign into, may be {@code null}
	 * @param autoboxing
	 *            whether to use implicit autoboxing/unboxing between primitives and wrappers
	 * @return {@code true} if assignment possible
	 */
	private static boolean isAssignable(Class<?>[] classArray, Class<?>[] toClassArray, final boolean autoboxing) {
		if (isSameLength(classArray, toClassArray) == false) return false;
		if (classArray == null) {
			classArray = new Class[0];
		}
		if (toClassArray == null) {
			toClassArray = new Class[0];
		}
		for (int i = 0; i < classArray.length; i++) {
			if (isAssignable(classArray[i], toClassArray[i], autoboxing) == false) return false;
		}
		return true;
	}

	/**
	 * <p>
	 * Checks whether two arrays are the same length, treating {@code null} arrays as length {@code 0}.
	 *
	 * <p>
	 * Any multi-dimensional aspects of the arrays are ignored.
	 * </p>
	 *
	 * @param array1
	 *            the first array, may be {@code null}
	 * @param array2
	 *            the second array, may be {@code null}
	 * @return {@code true} if length of arrays matches, treating {@code null} as an empty array
	 */
	private static boolean isSameLength(final Object[] array1, final Object[] array2) {
		if ((array1 == null && array2 != null && array2.length > 0) || (array2 == null && array1 != null && array1.length > 0)
				|| (array1 != null && array2 != null && array1.length != array2.length)) return false;
		return true;
	}

	/**
	 * Gets the number of steps required to promote a primitive number to another type.
	 * 
	 * @param from
	 *            the (primitive) source class
	 * @param to
	 *            the (primitive) destination class
	 * @return The cost of promoting the primitive
	 */
	private static float getPrimitivePromotionCost(final Class<?> from, final Class<?> to) {
		float cost = 0.0f;
		Class<?> cls = from;
		if (!cls.isPrimitive()) {
			// slight unwrapping penalty
			cost += 0.1f;
			cls = Values.wrapperToPrimitive(cls);
		}
		for (int i = 0; cls != to && i < ORDERED_PRIMITIVE_TYPES.length; i++) {
			if (cls == ORDERED_PRIMITIVE_TYPES[i]) {
				cost += 0.1f;
				if (i < ORDERED_PRIMITIVE_TYPES.length - 1) {
					cls = ORDERED_PRIMITIVE_TYPES[i + 1];
				}
			}
		}
		return cost;
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
				if (result == null || compareParameterTypes(ctor.getParameterTypes(), result.getParameterTypes(), parameterTypes) < 0)
					result = (Constructor<T>) ctor;
			}
		return result;
	}

	/**
	 * <p>
	 * Finds an accessible method that matches the given name and has compatible parameters. Compatible parameters mean that every method
	 * parameter is assignable from the given parameters. In other words, it finds a method with the given name that will take the
	 * parameters given.
	 * </p>
	 *
	 * <p>
	 * This method is used by {@link #invokeMethod(Object object, String methodName, Object[] args, Class[] parameterTypes)} .
	 * </p>
	 *
	 * <p>
	 * This method can match primitive parameter by passing in wrapper classes. For example, a {@code Boolean} will match a primitive
	 * {@code boolean} parameter.
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
	 * Gets the number of steps required needed to turn the source class into the destination class. This represents the number of steps in
	 * the object hierarchy graph.
	 * 
	 * @param from
	 *            The source class
	 * @param to
	 *            The destination class
	 * @return The cost of transforming an object
	 */
	private static float getObjectTransformationCost(Class<?> from, final Class<?> to) {
		if (to.isPrimitive()) return getPrimitivePromotionCost(from, to);
		float cost = 0.0f;
		while (from != null && !to.equals(from)) {
			if (to.isInterface() && isAssignable(from, to)) {
				// slight penalty for interface match.
				// we still want an exact match to override an interface match,
				// but
				// an interface match should override anything where we have to
				// get a superclass.
				cost += 0.25f;
				break;
			}
			cost++;
			from = from.getSuperclass();
		}
		/*
		 * If the destination class is null, we've travelled all the way up to an Object match. We'll penalize this by adding 1.5 to the
		 * cost.
		 */
		if (from == null) {
			cost += 1.5f;
		}
		return cost;
	}

	/**
	 * <p>
	 * Checks if one {@code Class} can be assigned to a variable of another {@code Class}.
	 * </p>
	 *
	 * <p>
	 * Unlike the {@link Class#isAssignableFrom(java.lang.Class)} method, this method takes into account widenings of primitive classes and
	 * {@code null} s.
	 * </p>
	 *
	 * <p>
	 * Primitive widenings allow an int to be assigned to a long, float or double. This method returns the correct result for these cases.
	 * </p>
	 *
	 * <p>
	 * {@code Null} may be assigned to any reference type. This method will return {@code true} if {@code null} is passed in and the toClass
	 * is non-primitive.
	 * </p>
	 *
	 * <p>
	 * Specifically, this method tests whether the type represented by the specified {@code Class} parameter can be converted to the type
	 * represented by this {@code Class} object via an identity conversion widening primitive or widening reference conversion. See
	 * <em><a href="http://docs.oracle.com/javase/specs/">The Java Language Specification</a></em> , sections 5.1.1, 5.1.2 and 5.1.4 for
	 * details.
	 * </p>
	 *
	 * <p>
	 * <strong>Since Lang 3.0,</strong> this method will default behavior for calculating assignability between primitive and wrapper types
	 * <em>corresponding to the running Java version</em>; i.e. autoboxing will be the default behavior in VMs running Java versions &gt;
	 * 1.5.
	 * </p>
	 *
	 * @param cls
	 *            the Class to check, may be null
	 * @param toClass
	 *            the Class to try to assign into, returns false if null
	 * @return {@code true} if assignment possible
	 */
	private static boolean isAssignable(final Class<?> cls, final Class<?> toClass) {
		return isAssignable(cls, toClass, true/*
												 * SystemUtils. isJavaVersionAtLeast( JavaVersion.JAVA_1_5)
												 */);
	}

	/**
	 * XXX Default access superclass workaround.
	 *
	 * When a {@code public} class has a default access superclass with {@code public} members, these members are accessible. Calling them
	 * from compiled code works fine. Unfortunately, on some JVMs, using reflection to invoke these members seems to (wrongly) prevent
	 * access even when the modifier is {@code public}. Calling {@code setAccessible(true)} solves the problem but will only work from
	 * sufficiently privileged code. Better workarounds would be gratefully accepted.
	 * 
	 * @param o
	 *            the AccessibleObject to set as accessible
	 * @return a boolean indicating whether the accessibility of the object was set to true.
	 */
	private static boolean setAccessibleWorkaround(final AccessibleObject o) {
		if (o == null || Refs.accessible(o)) return false;
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
	 * Compares the relative fitness of two sets of parameter types in terms of matching a third set of runtime parameter types, such that a
	 * list ordered by the results of the comparison would return the best match first (least).
	 *
	 * @param left
	 *            the "left" parameter set
	 * @param right
	 *            the "right" parameter set
	 * @param actual
	 *            the runtime parameter types to match against {@code left}/ {@code right}
	 * @return int consistent with {@code compare} semantics
	 */
	private static int compareParameterTypes(final Class<?>[] left, final Class<?>[] right, final Class<?>[] actual) {
		final float leftCost = getTotalTransformationCost(actual, left);
		final float rightCost = getTotalTransformationCost(actual, right);
		return leftCost < rightCost ? -1 : rightCost < leftCost ? 1 : 0;
	}

	/**
	 * <p>
	 * Returns an accessible method (that is, one that can be invoked via reflection) that implements the specified Method. If no such
	 * method can be found, return {@code null}.
	 * </p>
	 *
	 * @param method
	 *            The method that we wish to call
	 * @return The accessible method
	 */
	private static Method getAccessibleMethod(Method method) {
		if (!isAccessible(method)) return null;
		// If the declaring class is public, we are done
		final Class<?> cls = method.getDeclaringClass();
		if (Modifier.isPublic(cls.getModifiers())) return method;
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
	 * Returns the sum of the object transformation cost for each class in the source argument list.
	 * 
	 * @param from
	 *            The source arguments
	 * @param to
	 *            The destination arguments
	 * @return The total transformation cost
	 */
	private static float getTotalTransformationCost(final Class<?>[] from, final Class<?>[] to) {
		float totalCost = 0.0f;
		for (int i = 0; i < from.length; i++) {
			Class<?> fromc, toc;
			fromc = from[i];
			toc = to[i];
			totalCost += getObjectTransformationCost(fromc, toc);
		}
		return totalCost;
	}

	/**
	 * <p>
	 * Returns an accessible method (that is, one that can be invoked via reflection) that implements the specified method, by scanning
	 * through all implemented interfaces and subinterfaces. If no such method can be found, return {@code null}.
	 * </p>
	 *
	 * <p>
	 * There isn't any good reason why this method must be {@code private}. It is because there doesn't seem any reason why other classes
	 * should call this rather than the higher level methods.
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
					 * Swallow, if no method is found after the loop then this method returns null.
					 */
				}
				// Recursively check our parent interfaces
				Method method = getAccessibleMethodFromInterfaceNest(interfaces[i], methodName, parameterTypes);
				if (method != null) return method;
			}
		}
		return null;
	}

	/**
	 * <p>
	 * Returns an accessible method (that is, one that can be invoked via reflection) by scanning through the superclasses. If no such
	 * method can be found, return {@code null}.
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

	/**
	 * Returns whether a {@link Member} is accessible.
	 * 
	 * @param m
	 *            Member to check
	 * @return {@code true} if <code>m</code> is accessible
	 */
	private static boolean isAccessible(final Member m) {
		return m != null && Modifier.isPublic(m.getModifiers()) && !m.isSynthetic();
	}
}
