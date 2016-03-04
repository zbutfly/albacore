package net.butfly.albacore.dbo.cache;

import java.util.concurrent.locks.ReadWriteLock;

import org.apache.ibatis.cache.Cache;

public class NonCache implements Cache {
	private String id;

	public NonCache(String id) {
		this.id = id;
	}

	@Override
	public String getId() {
		return this.id;
	}

	@Override
	public void putObject(Object key, Object value) {}

	@Override
	public Object getObject(Object key) {
		return null;
	}

	@Override
	public Object removeObject(Object key) {
		return null;
	}

	@Override
	public void clear() {}

	@Override
	public int getSize() {
		return 0;
	}

	@Override
	public ReadWriteLock getReadWriteLock() {
		return null;
	}
}
