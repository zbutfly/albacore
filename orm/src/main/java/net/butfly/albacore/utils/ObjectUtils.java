package net.butfly.albacore.utils;

import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.support.AdvanceObjectSupport;
import net.butfly.albacore.support.ObjectSupport;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.DefaultObjectFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.DefaultObjectWrapperFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;

@SuppressWarnings("rawtypes")
public class ObjectUtils extends UtilsBase {
	public static ObjectSupport clone(ObjectSupport src, Class<? extends ObjectSupport> dstClass) {
		return clone(src, dstClass, true);
	}

	public static ObjectSupport clone(ObjectSupport src, Class<? extends ObjectSupport> dstClass, boolean cloneNull) {
		ObjectSupport dst = null;
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
		return object == null ? null : (T) ((AdvanceObjectSupport<?>) object).shadowClone();
	}

	@SuppressWarnings("unchecked")
	public static <D extends ObjectSupport<D>> D[] copy(ObjectSupport[] src, Class<D> clazz) {
		if (null == src) return null;
		D[] r = (D[]) Array.newInstance(clazz, src.length);
		for (int i = 0; i < src.length; i++) {
			r[i] = (D) clone(src[i], clazz);
		}
		return r;
	}

	public static void copy(ObjectSupport src, ObjectSupport dst) {
		copy(src, dst, true);
	}

	public static void copy(ObjectSupport src, ObjectSupport dst, boolean copyNull) {
		if (src == null) return;
		if (dst == null) throw new RuntimeException("Failure to copy a non-null object to a null instance.");
		MetaObject metaSrc = createMeta(src);
		MetaObject metaDst = createMeta(dst);
		for (String prop : metaSrc.getGetterNames())
			if (metaDst.hasSetter(prop)) {
				Object v = metaSrc.getValue(prop);
				if (copyNull || v != null) metaDst.setValue(prop, v);
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

	public static Map<? extends String, ? extends Object> toMap(Object target) {
		if (!(target instanceof MetaObject)) return toMap(createMeta(target));
		Map<String, Object> map = new HashMap<String, Object>();
		Set<Object> loopCheck = new HashSet<Object>();
		loopCheck.add(((MetaObject) target).getOriginalObject());
		for (String getter : ((MetaObject) target).getGetterNames()) {
			Object value = ((MetaObject) target).getValue(getter);
			if (loopCheck.contains(value)) continue;
			loopCheck.add(value);
			map.put(getter, value);
		}
		return map;
	}

	private static final ObjectFactory DEFAULT_OBJECT_FACTORY = new DefaultObjectFactory();
	private static final ObjectWrapperFactory DEFAULT_OBJECT_WRAPPER_FACTORY = new DefaultObjectWrapperFactory();

	@SuppressWarnings("unchecked")
	public static Object castValue(Object value, Class<?> dstClass) {
		if (null == value) return value;
		Class<?> srcClass = value.getClass();
		// 枚举类型和integer的互换
		if (TypeChecker.isEnum(dstClass) && TypeChecker.isInteger(srcClass)) return EnumUtils.parse((Class<Enum>) dstClass,
				((Number) value).byteValue());
		else if (TypeChecker.isInteger(dstClass) && TypeChecker.isEnum(srcClass)) return EnumUtils.value((Enum) value);
		// String和byte[]的互换
		else if (TypeChecker.isBytes(dstClass) && TypeChecker.isString(srcClass)) return ByteUtils.hex2byte((String) value);
		else if (TypeChecker.isString(dstClass) && TypeChecker.isBytes(srcClass)) return ByteUtils.byte2hex((byte[]) value);
		// integer和 String的互换
		else if (TypeChecker.isString(dstClass) && TypeChecker.isInteger(srcClass)) return Integer.toString(((Number) value)
				.intValue());
		else if (TypeChecker.isInteger(dstClass) && TypeChecker.isString(srcClass)) return Integer.parseInt(value.toString());
		else if (TypeChecker.isObjectSupportted(dstClass) && TypeChecker.isObjectSupportted(dstClass)) return clone(
				(ObjectSupport) value, (Class<ObjectSupport>) dstClass);
		else { // TODO: deeply copy, be done later.
				// 其他直接转换
			return value;
		}

	}

	public static <T> int compare(T o1, T o2) {
		if (null == o1 && null == o2) return 0;
		if (null == o1) return -1;
		if (null == o2) return 1;

		if (o1.getClass().isPrimitive())
			throw new NotImplementedException("Comparation between primitive objects is not implemented.");
		if (Number.class.isAssignableFrom(o1.getClass()) && Number.class.isAssignableFrom(o2.getClass()))
			return TypeComparators.numberComparator.compare((Number) o1, (Number) o2);
		if (o1.getClass().isArray() && o2.getClass().isArray())
			return TypeComparators.arrayComparator.compare((Object[]) o1, (Object[]) o2);
		if (Map.class.isAssignableFrom(o1.getClass()) && Map.class.isAssignableFrom(o2.getClass()))
			return TypeComparators.mapComparator.compare((Map) o1, (Map) o2);
		if (Iterable.class.isAssignableFrom(o1.getClass()) && Iterable.class.isAssignableFrom(o2.getClass()))
			return TypeComparators.iterableComparator.compare((Iterable) o1, (Iterable) o2);

		if (!o1.getClass().equals(o2.getClass()))
			throw new NotImplementedException("Comparation between objects with different classes is not implemented.");
		return TypeComparators.mapComparator.compare(toMap(o1), toMap(o2));
	}

	public static <T> boolean equals(T o1, T o2) {
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
				throw new NotImplementedException("Comparation between Numbers is not implemented.");
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

	private interface TypeChecker {
		static boolean isInteger(Class<?> clazz) {
			return int.class.equals(clazz) || Integer.class.equals(clazz);
		}

		static boolean isObjectSupportted(Class<?> clazz) {
			return ObjectSupport.class.isAssignableFrom(clazz);
		}

		static boolean isString(Class<?> clazz) {
			return String.class.equals(clazz);
		}

		static boolean isBytes(Class<?> clazz) {
			return byte[].class.equals(clazz);
		}

		static boolean isEnum(Class<?> clazz) {
			return Enum.class.isAssignableFrom(clazz);
		}
	}
}
