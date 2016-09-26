package net.butfly.albacore.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.lambda.Supplier;

/**
 * Replacement of "private static final ..."
 * 
 * @author butfly
 * @see net.butfly.albacore.utils.collection.LayerMap<V>
 */
public class Instances extends Utils {
	private static final Map<Object, Object> INSTANCE_POOL = new ConcurrentHashMap<Object, Object>();

	public static <T> T fetch(Class<T> instanceClass, Object... instanceKey) {
		return fetchFrom(instanceClass, INSTANCE_POOL, instanceClass, instanceKey, 0);
	}

	public static <T> T fetch(Supplier<T> supplier, Class<T> cl, Object... instanceKey) {
		return fetchFrom(supplier, INSTANCE_POOL, cl, instanceKey, 0);
	}

	@SuppressWarnings("unchecked")
	private static <T> T fetchFrom(Object instantiator, Map<Object, Object> pool, Object currKey, Object[] otherKeys, int pos) {
		boolean bottom = otherKeys.length <= pos;
		Object obj = pool.get(currKey);
		if (null == obj) {
			obj = bottom ? obj = construct(instantiator, otherKeys) : new ConcurrentHashMap<Object, Object>();
			if (null != obj) pool.put(currKey, obj);
			else throw new NullPointerException("Construction return null value.");
		}
		return (T) (bottom ? obj : fetchFrom(instantiator, (Map<Object, Object>) obj, otherKeys[pos], otherKeys, pos + 1));
	}

	@SuppressWarnings("unchecked")
	private static <T> T construct(Object instantiator, Object... params) {
		if (instantiator instanceof Class) return Reflections.construct((Class<T>) instantiator, params);
		if (instantiator instanceof Supplier) try {
			return ((Supplier<T>) instantiator).get();
		} catch (Exception e) {
			return null;
		}
		return null;
	}
}
