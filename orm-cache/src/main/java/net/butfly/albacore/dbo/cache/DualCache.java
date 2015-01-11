package net.butfly.albacore.dbo.cache;

import java.util.concurrent.locks.ReadWriteLock;

import org.apache.ibatis.cache.Cache;
import org.mybatis.caches.ehcache.EhcacheCache;
import org.mybatis.caches.memcached.MemcachedCache;

public final class DualCache implements Cache {
	private String id;
	private Cache firstCache, secondCache;

	public DualCache(final String id) {
		if (id == null) throw new IllegalArgumentException("Cache instances require an ID");
		this.id = id;
		this.firstCache = new EhcacheCache(id);
		this.secondCache = new MemcachedCache(id);
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
		// TODO Auto-generated method stub
		return null;
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
