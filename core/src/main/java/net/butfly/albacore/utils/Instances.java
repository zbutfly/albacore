package net.butfly.albacore.utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.google.common.reflect.TypeToken;

import net.butfly.albacore.lambda.Supplier;

/**
 * Replacement of "private static final ..."
 * 
 * @author butfly
 * @see net.butfly.albacore.utils.collection.LayerMap<V>
 */
@SuppressWarnings("unchecked")
public class Instances extends Utils {
	private static final Pool INSTANCE_POOL = new Pool();

	public static <T> T construct(Class<T> constructClass, Object... constructParams) {
		return fetch(() -> (T) Reflections.construct(constructClass, constructParams), constructClass, constructParams);
	}

	public static <T> T fetch(Supplier<T> supplier, Class<T> cl, Object... key) {
		return null == supplier ? fetch(cl, key)
				// : (T) fetchFrom(supplier, INSTANCE_POOL, key[0],
				// Arrays.copyOfRange(key, 1, key.length), 0);
				: (T) fetchFrom(supplier, INSTANCE_POOL, cl, key, 0);
	}

	public static <T> T fetch(Supplier<T> supplier, TypeToken<T> cl, Object... key) {
		return fetch(supplier, (Class<T>) cl.getRawType(), key);
	}

	public static <T> T fetch(TypeToken<T> cl, Object... key) {
		return fetch((Class<T>) cl.getRawType(), key);
	}

	public static <T> T fetch(Class<T> cl, Object... key) {
		try {
			if (key == null || key.length == 0) return (T) INSTANCE_POOL.get(cl);
			Pool curr = (Pool) INSTANCE_POOL.get(cl);
			for (int i = 0; i < key.length; i++) {
				Object k = null == key[i] ? "" : key[i];
				if (i == key.length - 1) return (T) curr.get(k);
				curr = (Pool) curr.get(k);
			}
			return null;
		} catch (Throwable th) {
			return null;
		}
	}

	private static <T> T fetchFrom(Supplier<T> supplier, Map<Object, Object> pool, Object currKey, Object[] allKeys, int pos) {
		boolean bottom = allKeys.length <= pos;
		currKey = null == currKey ? "" : currKey;
		if (bottom) return (T) pool.computeIfAbsent(currKey, k -> supplier.get());
		else {
			Map<Object, Object> subpool = (Map<Object, Object>) pool.computeIfAbsent(currKey, k -> new Pool());
			return fetchFrom(supplier, subpool, allKeys[pos], allKeys, pos + 1);
		}
	}

	private static final class Pool extends ConcurrentHashMap<Object, Object> {
		private static final long serialVersionUID = -8373236640411262551L;

		@Override
		public Object computeIfAbsent(Object key, Function<? super Object, ? extends Object> mappingFunction) {
			if (containsKey(key)) return get(key);
			else {
				Object value = mappingFunction.apply(key);
				if (null != value) put(key, value);
				return value;
			}
		}
	}
}
