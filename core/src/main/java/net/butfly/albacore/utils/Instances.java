package net.butfly.albacore.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.lambda.Supplier;

/**
 * Replacement of "private static final ..."
 * 
 * @author butfly
 * @see net.butfly.albacore.utils.collection.LayerMap<V>
 */
public class Instances extends Utils {
	private static final Map<Object, Object> INSTANCE_POOL = new ConcurrentHashMap<Object, Object>();

	@Deprecated
	public static <T> T construct(Class<T> constructClass, Object... constructParams) {
		return fetch(() -> (T) Reflections.construct(constructClass, constructParams), constructClass, constructParams, 0);
	}

	public static <T> T fetch(Supplier<T> supplier, Class<T> cl, Object... key) {
		return null == supplier ? fetch(cl, key) : fetchFrom(supplier, INSTANCE_POOL, cl, key, 0);
	}

	@SuppressWarnings("unchecked")
	public static <T> T fetch(Supplier<T> supplier, TypeToken<T> cl, Object... key) {
		return fetch(supplier, (Class<T>) cl.getRawType(), key);
	}

	@SuppressWarnings("unchecked")
	public static <T> T fetch(TypeToken<T> cl, Object... key) {
		return fetch((Class<T>) cl.getRawType(), key);
	}

	@SuppressWarnings("unchecked")
	public static <T> T fetch(Class<T> cl, Object... key) {
		try {
			if (key == null || key.length == 0) return (T) INSTANCE_POOL.get(cl);
			Map<Object, Object> curr = (Map<Object, Object>) INSTANCE_POOL.get(cl);
			for (int i = 0; i < key.length; i++) {
				Object k = null == key[i] ? "" : key[i];
				if (i == key.length - 1) return (T) curr.get(k);
				curr = (Map<Object, Object>) curr.get(k);
			}
			return null;
		} catch (Throwable th) {
			return null;
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> T fetchFrom(Supplier<T> supplier, Map<Object, Object> pool, Object currKey, Object[] otherKeys, int pos) {
		boolean bottom = otherKeys.length <= pos;
		currKey = null == currKey ? "" : currKey;
		Object obj = pool.get(currKey);
		if (null == obj) {
			obj = bottom ? obj = supplier.get() : new ConcurrentHashMap<Object, Object>();
			pool.put(currKey, obj);
		}
		return (T) (bottom ? obj : fetchFrom(supplier, (Map<Object, Object>) obj, otherKeys[pos], otherKeys, pos + 1));
	}
}
