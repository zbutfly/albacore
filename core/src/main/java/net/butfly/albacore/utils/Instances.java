package net.butfly.albacore.utils;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Instances extends Utils {
	private static final Map<Object, Object> INSTANCE_POOL = new InstancePool();

	public interface Instantiator<T> {
		T create();
	}

	public static <T> T fetch(Instantiator<T> instantiator, Object... instanceKey) {
		return fetchFrom(instantiator, INSTANCE_POOL, instantiator.getClass(), instanceKey);
	}

	@SuppressWarnings("unchecked")
	private static <T> T fetchFrom(Instantiator<T> instantiator, Map<Object, Object> pool, Object currKey, Object[] otherKeys) {
		boolean bottom = otherKeys == null || otherKeys.length == 0;
		Object result = pool.get(currKey);
		if (null == result) {
			result = bottom ? instantiator.create() : new InstancePool();
			if (null != result) pool.put(currKey, result);
		}
		return bottom ? (T) result : fetchFrom(instantiator, (Map<Object, Object>) result, otherKeys[0],
				Arrays.copyOfRange(otherKeys, 1, otherKeys.length));
	}

	private final static class InstancePool extends ConcurrentHashMap<Object, Object> {
		private static final long serialVersionUID = -7047645616308974050L;
	}
}
