package net.butfly.albacore.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import net.butfly.albacore.utils.async.Task;

/**
 * Replacement of "private static final ..."
 * 
 * @author butfly
 *
 */
public class Instances extends Utils {
	private static final Map<Object, Object> INSTANCE_POOL = new ConcurrentHashMap<Object, Object>();

	public static <T> T fetch(Class<T> instanceClass, Object... instanceKey) {
		return fetchFrom(instanceClass, INSTANCE_POOL, instanceClass, instanceKey, 0);
	}

	public static <T> T fetch(Task.Callable<T> instantiator, Object... instanceKey) {
		return fetchFrom(instantiator, INSTANCE_POOL, instantiator.getClass(), instanceKey, 0);
	}

	@SuppressWarnings("unchecked")
	private static <T> T fetchFrom(Object instantiator, Map<Object, Object> pool, Object currKey, Object[] otherKeys, int pos) {
		boolean bottom = otherKeys.length <= pos;
		Object obj = pool.get(currKey);
		if (null == obj) {
			obj = bottom ? obj = construct(instantiator, otherKeys) : new ConcurrentHashMap<Object, Object>();
			if (null != obj) pool.put(currKey, obj);
			else throw new NullPointerException("Construction return null lvalue.");
		}
		return (T) (bottom ? obj : fetchFrom(instantiator, (Map<Object, Object>) obj, otherKeys[pos], otherKeys, pos + 1));
	}

	@SuppressWarnings("unchecked")
	private static <T> T construct(Object instantiator, Object... paramsAndClasses) {
		if (instantiator instanceof Class) return Reflections.construct((Class<T>) instantiator, paramsAndClasses);
		if (instantiator instanceof Task.Callable) try {
			return ((Task.Callable<T>) instantiator).call();
		} catch (Exception e) {
			return null;
		}
		return null;
	}
}
