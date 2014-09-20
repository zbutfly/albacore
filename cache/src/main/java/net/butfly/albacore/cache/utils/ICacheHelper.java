package net.butfly.albacore.cache.utils;

public interface ICacheHelper {
	Object get(Key key);

	void set(Key key, Object value);

	boolean invalidate(Key key);

	boolean invalidate(Key key, int serviceType);

	void set(Key key, Object value, int serviceType);
}
