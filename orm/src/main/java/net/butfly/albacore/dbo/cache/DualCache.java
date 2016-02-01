package net.butfly.albacore.dbo.cache;

import java.util.concurrent.locks.ReadWriteLock;

import net.butfly.albacore.utils.Reflections;

import org.apache.ibatis.cache.Cache;

public final class DualCache implements Cache {
	private String id;
	private Class<? extends Cache> firstClass, secondClass;
	private Cache firstCache, secondCache;

	public DualCache(final String id) {
		if (id == null) throw new IllegalArgumentException("Cache instances require an ID");
		this.id = id;
		this.firstCache = Reflections.construct(this.firstClass, id);
		this.secondCache = Reflections.construct(this.secondClass, id);
	}

	@Override
	public String getId() {
		return this.id;
	}

	@Override
	public void putObject(Object key, Object value) {

	}

	@Override
	public Object getObject(Object key) {
		Object r = this.firstCache.getObject(key);
		return null == r ? this.secondCache.getObject(key) : r;
	}

	@Override
	public Object removeObject(Object key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub

	}

	@Override
	public int getSize() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public ReadWriteLock getReadWriteLock() {
		// TODO Auto-generated method stub
		return null;
	}

}
