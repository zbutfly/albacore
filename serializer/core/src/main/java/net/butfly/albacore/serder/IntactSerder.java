package net.butfly.albacore.serder;

public class IntactSerder<V> implements ClassInfoSerder<V, V> {
	private static final long serialVersionUID = -5841357721404185556L;

	@Override
	public V ser(V from) {
		return from;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends V> T der(V from) {
		return (T) from;
	}
}
