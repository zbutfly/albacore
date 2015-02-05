package net.butfly.albacore.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Replacement of "private static final ..."
 * 
 * @author butfly
 *
 */
public class Instances extends Utils {
	private static final Map<Object, Object> INSTANCE_POOL = new ConcurrentHashMap<Object, Object>();

	public interface Instantiator<T> {
		T create();
	}

	public static <T> T fetch(Instantiator<T> instantiator, Object... instanceKey) {
		return fetchFrom(instantiator, INSTANCE_POOL, instantiator.getClass(), instanceKey, 0);
	}

	@SuppressWarnings("unchecked")
	private static <T> T fetchFrom(Instantiator<T> instantiator, Map<Object, Object> pool, Object currKey, Object[] otherKeys,
			int pos) {
		boolean bottom = otherKeys.length <= pos;
		Object result = pool.get(currKey);
		if (null == result) {
			result = bottom ? instantiator.create() : new ConcurrentHashMap<Object, Object>();
			if (null != result) pool.put(currKey, result);
		}
		return bottom ? (T) result : fetchFrom(instantiator, (Map<Object, Object>) result, otherKeys[pos], otherKeys, pos + 1);
	}
}
