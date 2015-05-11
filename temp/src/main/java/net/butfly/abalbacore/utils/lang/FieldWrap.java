package net.butfly.abalbacore.utils.lang;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class FieldWrap {
	private String name;
	private Class<?> type;
	private Field field = null;
	private Method readMethod = null;
	private Method writeMethod = null;
	private Method itemReadMethod = null;
	private Method itemWriteMethod = null;
	private boolean readable;
	private boolean writable;
	private boolean multiple; // for array and iteratable
	private boolean complex; // for map and object

	public FieldWrap(Class<?> targetClass, String fieldName) {
		this.field = searchField(targetClass, fieldName);
		this.name = fieldName;
	}

	public FieldWrap(Field field) {
		this.field = field;
		this.name = field.getName();
	}

	private static boolean isRootClass(Class<?> targetClass) {
		return targetClass == null || Object.class.equals(targetClass) || Class.class.isAssignableFrom(targetClass);
	}

	private static Field searchField(Class<?> targetClass, String fieldName) {
		if (isRootClass(targetClass)) return null;
		Field f = null;
		try {
			f = targetClass.getDeclaredField(fieldName);
		} catch (Exception e) {}
		if (null != f) return f;

		f = searchField(targetClass.getSuperclass(), fieldName);
		if (null != f) return f;

		for (Class<?> intf : targetClass.getInterfaces()) {
			f = searchField(intf, fieldName);
			if (null != f) return f;
		}
		return null;
	}

	private static Method searchMethod(Class<?> targetClass, String methodName, Class<?>... argClass) {
		if (targetClass == null) return null;
		Method f = null;
		try {
			f = targetClass.getDeclaredMethod(methodName, argClass);
		} catch (Exception e) {}
		if (null != f) return f;

		f = searchMethod(targetClass.getSuperclass(), methodName, argClass);
		if (null != f) return f;

		for (Class<?> intf : targetClass.getInterfaces()) {
			f = searchMethod(intf, methodName, argClass);
			if (null != f) return f;
		}
		return null;
	}

	public Class<?> getType() {
		return type;
	}

	public String getName() {
		return this.name;
	}

	public Object getValue(Object target) {
		if (null != this.field) {
			boolean acc = this.field.isAccessible();
			if (!acc) this.field.setAccessible(true);
			try {
				return this.field.get(target);
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if (!acc) this.field.setAccessible(false);
			}
		}
		return null;
	}

	public void set(Object target, Object value) {
		// TODO Auto-generated method stub

	}

	public static FieldWrap[] getAllFields(Class<? extends Object> targetClass) {
		Map<String, FieldWrap> fields = new HashMap<String, FieldWrap>();
		Class<? extends Object> c = targetClass;
		while (!isRootClass(c)) {
			for (Field f : c.getDeclaredFields()) {
				int mod = f.getModifiers();
				if (!Modifier.isTransient(mod) && !Modifier.isFinal(mod) && !fields.containsKey(f.getName()))
					fields.put(f.getName(), new FieldWrap(f));
			}
			c = c.getSuperclass();
		}

		List<Class<?>> intfs = new ArrayList<Class<?>>();
		for (Class<?> intf : targetClass.getInterfaces())
			intfs.add(intf);
		int i = 0;
		while (i < intfs.size()) {
			Class<?> curr = intfs.get(0);
			for (Class<?> intf : curr.getInterfaces())
				intfs.add(intf);
			i++;
		}
		for (Class<?> intf : intfs)
			for (Field f : intf.getDeclaredFields()) {
				int mod = f.getModifiers();
				if (!Modifier.isTransient(mod) && !Modifier.isFinal(mod) && !fields.containsKey(f.getName()))
					fields.put(f.getName(), new FieldWrap(f));
			}
		return fields.values().toArray(new FieldWrap[fields.size()]);
	}

	private static void addAllFields(Map<String, FieldWrap> fieldMap, Class<? extends Object> targetClass) {
		Class<? extends Object> c = targetClass;
		while (!isRootClass(targetClass)) {
			for (Field f : targetClass.getDeclaredFields()) {
				int mod = f.getModifiers();
				if (!Modifier.isTransient(mod) && !Modifier.isFinal(mod) && !fieldMap.containsKey(f.getName()))
					fieldMap.put(f.getName(), new FieldWrap(f));
			}
			c = targetClass.getSuperclass();
		}
		for (Class<?> intf : targetClass.getInterfaces()) {
			addAllFields(fieldMap, intf);
		}
	}
}
