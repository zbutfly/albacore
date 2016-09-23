package net.butfly.albacore.serder;

import com.google.common.reflect.TypeToken;

public class IntactSerder<V> implements Serder<V, V> {
	private static final long serialVersionUID = -5841357721404185556L;

	@Override
	public <T extends V> V ser(T from) {
		return from;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends V> T der(V from, TypeToken<T> to) {
		return (T) from;
	}
}
