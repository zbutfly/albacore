package net.butfly.albacore.utils;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.support.Bean;
import net.butfly.albacore.support.Beans;
import net.butfly.albacore.utils.imports.meta.MetaObject;
import net.butfly.albacore.utils.imports.meta.factory.DefaultObjectFactory;
import net.butfly.albacore.utils.imports.meta.factory.ObjectFactory;
import net.butfly.albacore.utils.imports.meta.wrapper.DefaultObjectWrapperFactory;
import net.butfly.albacore.utils.imports.meta.wrapper.ObjectWrapperFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.google.common.base.Defaults;

@SuppressWarnings("rawtypes")
public class ObjectUtils extends UtilsBase {
	public static Beans clone(Beans src, Class<? extends Beans> dstClass) {
		return clone(src, dstClass, true);
	}

	public static Beans clone(Beans src, Class<? extends Beans> dstClass, boolean cloneNull) {
		Beans dst = null;
		try {
			dst = dstClass.newInstance();
		} catch (Exception ex) {
			throw new RuntimeException("Failure create an instance for class: " + dstClass.getName());
		}
		copy(dst, src, cloneNull);
		return dst;
	}

	@SuppressWarnings("unchecked")
	public static <T> T shadowClone(T object) {
		return object == null ? null : (T) ((Bean<?>) object).shadowClone();
	}

	@SuppressWarnings("unchecked")
	public static <D extends Beans<D>> D[] copy(Beans[] src, Class<D> clazz) {
		if (null == src) return null;
		D[] r = (D[]) Array.newInstance(clazz, src.length);
		for (int i = 0; i < src.length; i++) {
			r[i] = (D) clone(src[i], clazz);
		}
		return r;
	}

	public static void copy(Beans src, Beans dst) {
		copy(src, dst, true);
	}

	public static void copy(Beans src, Beans dst, boolean copyNull) {
		if (src == null) return;
		if (dst == null) throw new RuntimeException("Failure to copy a non-null object to a null instance.");
		MetaObject metaSrc = createMeta(src);
		MetaObject metaDst = createMeta(dst);
		for (String prop : metaSrc.getGetterNames())
			if (metaDst.hasSetter(prop)) {
				Object v = metaSrc.getValue(prop);
				Object val = castValue(v, metaDst.getSetterType(prop));
				if (copyNull || (v != null && val != null)) metaDst.setValue(prop, val);
			}
	}

	public static final MetaObject createMeta(Object target) {
		MetaObject meta = MetaObject.forObject(target, DEFAULT_OBJECT_FACTORY, DEFAULT_OBJECT_WRAPPER_FACTORY);
		while (meta.hasGetter("target") || meta.hasGetter("h")) {
			while (meta.hasGetter("h")) {
				Object object = meta.getValue("h");
				meta = MetaObject.forObject(object, DEFAULT_OBJECT_FACTORY, DEFAULT_OBJECT_WRAPPER_FACTORY);
			}
			while (meta.hasGetter("target")) {
				Object object = meta.getValue("target");
				meta = MetaObject.forObject(object, DEFAULT_OBJECT_FACTORY, DEFAULT_OBJECT_WRAPPER_FACTORY);
			}
		}
		return meta;
	}

	public static Map<String, Object> toMap(Object target) {
		if (!(target instanceof MetaObject)) return toMap(createMeta(target));
		Map<String, Object> map = new HashMap<String, Object>();
		MetaObject meta = (MetaObject) target;
		// TODO: avoid cache, use "transient"
		Set<Object> objectPool = new HashSet<Object>();
		objectPool.add(meta.getOriginalObject());
		for (String getter : meta.getGetterNames()) {
			Object value = meta.getValue(getter);
			PrimaryCategory cat = TypeChecker.getPrimaryCategory(meta.getGetterType(getter));
			if (cat == PrimaryCategory.STRING || cat == PrimaryCategory.NUMBER) map.put(getter, value);
			else if (!objectPool.contains(value)) {
				objectPool.add(value);
				map.put(getter, value);
			}
		}
		return map;
	}

	public static void fromMap(Object target, Map<String, Object> map) {
		if (target == null) throw new NullPointerException();
		if (Class.class.isAssignableFrom(target.getClass())) try {
			target = ((Class) target).newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		MetaObject meta;
		if (target instanceof MetaObject) meta = (MetaObject) target;
		else meta = createMeta(target);
		for (String setter : meta.getSetterNames())
			if (map.containsKey(setter)) meta.setValue(setter, map.get(setter));
	}

	private static final ObjectFactory DEFAULT_OBJECT_FACTORY = new DefaultObjectFactory();
	private static final ObjectWrapperFactory DEFAULT_OBJECT_WRAPPER_FACTORY = new DefaultObjectWrapperFactory();

	@SuppressWarnings("unchecked")
	public static Object castValue(Object value, Class<?> dstClass) {
		if (dstClass == null) return value;
		if (null == value) return dstClass.isPrimitive() ? Defaults.defaultValue(dstClass) : null;

		Class<?> srcClass = value.getClass();
		if (dstClass.isAssignableFrom(srcClass)) return value;

		PrimaryCategory srcCat = TypeChecker.getPrimaryCategory(srcClass);
		PrimaryCategory dstCat = TypeChecker.getPrimaryCategory(dstClass);

		switch (srcCat) {
		case BOOL: {
			switch (dstCat) {
			case BOOL:
				return value;
			case NUMBER:
				return value != Defaults.defaultValue(dstClass);
			case STRING:
				return Boolean.parseBoolean((String) value);
			default:
				return Defaults.defaultValue(dstClass);
			}
		}
		case ENUM:
			switch (dstCat) {
			case ENUM:
				return EnumUtils.parse((Class<Enum>) dstClass, EnumUtils.value((Enum) value));
			case NUMBER:
				return EnumUtils.value((Enum) value);
			case STRING:
				return ((Enum) value).name();
			default:
				return Defaults.defaultValue(dstClass);
			}
		case NUMBER:
			NumberCategory srcNumCat = NumberCategory.whichNumber(srcClass);
			switch (dstCat) {
			case NUMBER:
				// TODO: different number type casting!
				return value;
			case ENUM:
				return EnumUtils.parse((Class<Enum>) dstClass, byte.class.cast(srcNumCat.primitiveClass.cast(value)));
			case STRING:
				return srcNumCat.numberClass.cast(value).toString();
			default:
				return Defaults.defaultValue(dstClass);
			}
		case STRING:
			switch (dstCat) {
			case NUMBER:
				NumberCategory dstNumCat = NumberCategory.whichNumber(dstClass);
				Method vof;
				try {
					vof = dstNumCat.numberClass.getMethod("valueOf", String.class);
				} catch (Exception e) {
					return Defaults.defaultValue(dstClass);
				}
				if (null == vof || !Modifier.isStatic(vof.getModifiers()))
					throw new IllegalArgumentException("Could not parse Number class: " + dstNumCat.numberClass.getName());
				try {
					return vof.invoke(null, value);
				} catch (Exception e) {
					return Defaults.defaultValue(dstClass);
				}
			case ENUM:
				return Enum.valueOf((Class<Enum>) dstClass, (String) value);
			case STRING:
				return (String) value;
			default:
				return Defaults.defaultValue(dstClass);
			}
		case OBJECT_MAP:
			switch (dstCat) {
			case STRING:
				return value.toString();
			case OBJECT_MAP:
				return clone((Beans) value, (Class<? extends Beans>) dstClass);
			default:
				return Defaults.defaultValue(dstClass);
			}
		case ORIGINAL_OBJ:
			switch (dstCat) {
			case STRING:
				return value.toString();
			case ORIGINAL_OBJ:
				return value;
			default:
				return Defaults.defaultValue(dstClass);
			}
		case ARRAY_COLLECTION:
			switch (dstCat) {
			case ARRAY_COLLECTION:
				if (srcClass.isArray()) {
					int len = ReflectionUtils.safeFieldGet(value, "length");
					if (dstClass.isArray()) { // source is an Array
						Object dst = Array.newInstance(dstClass.getComponentType(), len);
						for (int i = 0; i < len; i++)
							Array.set(dst, i, castValue(Array.get(value, i), dstClass.getComponentType()));
						return dst;
					} else if (Collection.class.isAssignableFrom(dstClass)) {
						Class<?> dstComponentType = TypeChecker.getIterableClass(dstClass);
						Collection dst;
						try {
							dst = (Collection) dstClass.newInstance();
						} catch (Exception e) {
							return Defaults.defaultValue(dstClass);
						}
						for (int i = 0; i < len; i++)
							dst.add(castValue(Array.get(value, i), dstComponentType));
						return dst;
					} else return Defaults.defaultValue(dstClass);
				} else {
					if (Collection.class.isAssignableFrom(srcClass)) { // source is a Collection
						Collection co = (Collection) value;
						Iterator it = co.iterator();
						int size = co.size();
						if (dstClass.isArray()) {
							Object dst = Array.newInstance(dstClass.getComponentType(), size);
							for (int i = 0; i < size; i++)
								Array.set(dst, i, castValue(it.next(), dstClass.getComponentType()));
							return dst;
						} else if (Collection.class.isAssignableFrom(dstClass)) {
							Collection dst;
							try {
								dst = (Collection) dstClass.newInstance();
							} catch (Exception e) {
								return Defaults.defaultValue(dstClass);
							}
							Class<?> dstComponentType = TypeChecker.getIterableClass(dstClass);
							for (int i = 0; i < size; i++)
								dst.add(castValue(it.next(), dstComponentType));
							return dst;
						} else return Defaults.defaultValue(dstClass);
					} else { // source is an Iterable
						Iterable itt = (Iterable) value;
						Iterator it = itt.iterator();
						Class<?> srcComponentType = TypeChecker.getIterableClass(srcClass);
						Class<?> dstComponentType = TypeChecker.getIterableClass(dstClass);
						if (dstClass.isArray()) {
							ArrayList dst = new ArrayList();
							while (it.hasNext())
								dst.add(castValue(castValue(it.next(), dstComponentType), dstClass.getComponentType()));
							return dst.toArray((Object[]) Array.newInstance(srcComponentType, dst.size()));
						} else if (Collection.class.isAssignableFrom(dstClass)) {
							Collection dst;
							try {
								dst = (Collection) dstClass.newInstance();
							} catch (Exception e) {
								return Defaults.defaultValue(dstClass);
							}
							while (it.hasNext())
								dst.add(castValue(it.next(), dstComponentType));
							return dst;
						} else return Defaults.defaultValue(dstClass);
					}
				}
			default:
				return null;
			}

		}
		return null;
	}

	public static <T1, T2> int compare(T1 o1, T2 o2) {
		if (null == o1 && null == o2) return 0;
		if (null == o1) return -1;
		if (null == o2) return 1;

		if (o1.getClass().isPrimitive()) throw new NotImplementedException();
		if (Number.class.isAssignableFrom(o1.getClass()) && Number.class.isAssignableFrom(o2.getClass()))
			return TypeComparators.numberComparator.compare((Number) o1, (Number) o2);
		if (o1.getClass().isArray() && o2.getClass().isArray())
			return TypeComparators.arrayComparator.compare((Object[]) o1, (Object[]) o2);
		if (Map.class.isAssignableFrom(o1.getClass()) && Map.class.isAssignableFrom(o2.getClass()))
			return TypeComparators.mapComparator.compare((Map) o1, (Map) o2);
		if (Iterable.class.isAssignableFrom(o1.getClass()) && Iterable.class.isAssignableFrom(o2.getClass()))
			return TypeComparators.iterableComparator.compare((Iterable) o1, (Iterable) o2);

		return TypeComparators.mapComparator.compare(toMap(o1), toMap(o2));
	}

	public static <T1, T2> boolean equals(T1 o1, T2 o2) {
		if (null == o1 && null == o2) return true;
		if (null == o1 || null == o2) return false;
		if (o1.getClass().isPrimitive()) return o1 == o2;
		if (Number.class.isAssignableFrom(o1.getClass()) && Number.class.isAssignableFrom(o2.getClass())) return o1.equals(o2);
		if (o1.getClass().isArray() && o2.getClass().isArray())
			return TypeComparators.arrayComparator.compare((Object[]) o1, (Object[]) o2) == 0;
		if (Map.class.isAssignableFrom(o1.getClass()) && Map.class.isAssignableFrom(o2.getClass()))
			return TypeComparators.mapComparator.compare((Map) o1, (Map) o2) == 0;
		if (Iterable.class.isAssignableFrom(o1.getClass()) && Iterable.class.isAssignableFrom(o2.getClass()))
			return TypeComparators.iterableComparator.compare((Iterable) o1, (Iterable) o2) == 0;

		if (!o1.getClass().equals(o2.getClass())) return false;
		return TypeComparators.mapComparator.compare(toMap(o1), toMap(o2)) == 0;
	}

	private interface TypeComparators {
		static final Comparator<Number> numberComparator = new Comparator<Number>() {
			@Override
			public int compare(Number o1, Number o2) {
				throw new NotImplementedException();
			}
		};
		static final Comparator<Iterable<?>> iterableComparator = new Comparator<Iterable<?>>() {
			@Override
			public int compare(Iterable<?> o1, Iterable<?> o2) {
				if (null == o1 && null == o2) return 0;
				if (null == o1) return -1;
				if (null == o2) return 1;
				Iterator<?> it1 = o1.iterator(), it2 = o2.iterator();
				while (it1.hasNext() && it2.hasNext()) {
					int r = ObjectUtils.compare(it1.next(), it2.next());
					if (r != 0) return r;
				}
				if (it1.hasNext()) return 1;
				if (it2.hasNext()) return -1;
				return 0;
			}
		};
		static final Comparator<Object[]> arrayComparator = new Comparator<Object[]>() {
			@Override
			public int compare(Object[] o1, Object[] o2) {
				if (null == o1 && null == o2) return 0;
				if (null == o1) return -1;
				if (null == o2) return 1;
				for (int i = 0; i < Math.min(o1.length, o2.length); i++) {
					int r = ObjectUtils.compare(o1[i], o2[i]);
					if (r != 0) return r;
				}
				if (o1.length > o2.length) return 1;
				if (o1.length < o2.length) return -1;
				return 0;
			}
		};
		static final Comparator<Map<?, ?>> mapComparator = new Comparator<Map<?, ?>>() {
			@Override
			public int compare(Map<?, ?> o1, Map<?, ?> o2) {
				if (null == o1 && null == o2) return 0;
				if (null == o1) return -1;
				if (null == o2) return 1;
				return iterableComparator.compare(o1.entrySet(), o2.entrySet());
			}
		};
	}

	private enum PrimaryCategory {
		STRING, NUMBER, ARRAY_COLLECTION, OBJECT_MAP, ORIGINAL_OBJ, ENUM, BOOL
	}

	private static final Set<Class<?>> ALL_NUMBER_CLASSES = new HashSet<Class<?>>();

	private enum NumberCategory {
		INT(int.class, Integer.class), LONG(long.class, Long.class), BYTE(byte.class, Byte.class), SHORT(short.class,
				Short.class), FLOAT(float.class, Float.class), DOUBLE(double.class, Double.class), NUMBER(null, null);
		private Class<?> primitiveClass;
		private Class<? extends Number> numberClass;

		// private Method valueMethod;

		NumberCategory(Class<?> primitiveClass, Class<? extends Number> numberClass) {
			this.primitiveClass = primitiveClass;
			this.numberClass = numberClass;
			// if (this.numberClass != null && this.primitiveClass != null)
			// for (Method m : numberClass.getDeclaredMethods())
			// if (m.getName().endsWith("Value") && m.getParameterTypes().length == 0
			// && m.getReturnType().equals(this.primitiveClass)) {
			// this.valueMethod = m;
			// break;
			// }

			if (primitiveClass != null) add(primitiveClass);
			if (numberClass != null) add(numberClass);
		}

		private void add(Class<?> cl) {
			if (null != cl) ALL_NUMBER_CLASSES.add(cl);
		}

		static boolean isNumber(Class<?> clazz) {
			return ALL_NUMBER_CLASSES.contains(clazz);
		}

		static NumberCategory whichNumber(Class<?> clazz) {
			for (NumberCategory cat : NumberCategory.values()) {
				if (cat.primitiveClass.equals(clazz) || cat.numberClass.isAssignableFrom(clazz)) return cat;
			}
			return null;
		}
	}

	private static class TypeChecker {
		static PrimaryCategory getPrimaryCategory(Class<?> clazz) {
			if (Enum.class.isAssignableFrom(clazz)) return PrimaryCategory.ENUM;
			if (boolean.class.equals(clazz) || Boolean.class.equals(clazz)) return PrimaryCategory.BOOL;
			if (NumberCategory.isNumber(clazz)) return PrimaryCategory.NUMBER;
			if (String.class.equals(clazz) || char.class.equals(clazz) || Character.class.equals(clazz))
				return PrimaryCategory.STRING;
			if (clazz.isArray() || Iterable.class.isAssignableFrom(clazz)) return PrimaryCategory.ARRAY_COLLECTION;
			if (Map.class.isAssignableFrom(clazz) || Beans.class.isAssignableFrom(clazz)) return PrimaryCategory.OBJECT_MAP;
			return PrimaryCategory.ORIGINAL_OBJ;
		}

		static Class<?> getIterableClass(Class<?> clazz) {
			if (clazz.isArray()) return clazz.getComponentType();
			if (Iterable.class.isAssignableFrom(clazz)) {
				Class<?> cl = GenericUtils.getGenericParamClass(clazz, Iterable.class, "T");
				return null == cl ? Object.class : cl;
			} else return null;
		}
	}
}
