package net.butfly.albacore.utils.collection;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * dynamic multiple type keys map implementation.
 * 
 * @author butfly
 */
public class LayerMap<V> implements Map<Object[], V> {
	private final Map<Object, Object> pool = new HashMap<Object, Object>();

	@Override
	public int size() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isEmpty() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsKey(Object key) {
		return get(key) != null;
	}

	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public V get(Object key) {
		if (null == key) throw new NullPointerException();
		return key.getClass().isArray() ? get((Object[]) key) : get(new Object[] { key });
	}

	public V get(Object... key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public V put(Object[] key, V value) {
		return this.put(value, key);
	}

	public V put(V value, Object... key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public V remove(Object key) {
		return key.getClass().isArray() ? remove((Object[]) key) : remove(new Object[] { key });
	}

	public V remove(Object... key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void putAll(Map<? extends Object[], ? extends V> m) {
		for (Entry<? extends Object[], ? extends V> e : m.entrySet())
			put(e.getKey(), e.getValue());
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void clear() {
		for (Entry<Object, Object> e : pool.entrySet())
			if (e.getValue() != null && Map.class.isAssignableFrom(e.getValue().getClass())) ((Map) e.getValue()).clear();
		pool.clear();
	}

	@Override
	public Set<Object[]> keySet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<V> values() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<java.util.Map.Entry<Object[], V>> entrySet() {
		// TODO Auto-generated method stub
		return null;
	}

}
