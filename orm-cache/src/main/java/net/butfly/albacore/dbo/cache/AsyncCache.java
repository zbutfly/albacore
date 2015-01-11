package net.butfly.albacore.dbo.cache;

import java.util.concurrent.locks.ReadWriteLock;

import net.butfly.albacore.utils.async.AsyncUtils;
import net.butfly.albacore.utils.async.Callable;
import net.butfly.albacore.utils.async.Signal;
import net.butfly.albacore.utils.async.Task;

import org.apache.ibatis.cache.Cache;
import org.mybatis.caches.memcached.MemcachedCache;

public class AsyncCache implements Cache {
	private Cache delegate;

	public AsyncCache(final String id) {
		try {
			this.delegate = new MemcachedCache(id);
		} catch (Exception ex) {
			this.delegate = new NonCache(id);
		}
	}

	@Override
	public String getId() {
		return this.delegate.getId();
	}

	@Override
	public void putObject(final Object key, final Object value) {
		if (null == this.delegate) return;
		try {
			AsyncUtils.execute(new Task<Void>(new Callable<Void>() {
				@Override
				public Void call() throws Signal {
					delegate.putObject(key, value);
					return null;
				}
			}));
		} catch (Signal e) {
			throw new RuntimeException(e.getCause());
		}
	}

	@Override
	public Object getObject(final Object key) {
		try {
			return AsyncUtils.execute(new Task<Object>(new Callable<Object>() {
				@Override
				public Object call() throws Signal {
					return delegate.getObject(key);
				}
			}));
		} catch (Signal e) {
			throw new RuntimeException(e.getCause());
		}
	}

	@Override
	public Object removeObject(final Object key) {
		try {
			return AsyncUtils.execute(new Task<Object>(new Callable<Object>() {
				@Override
				public Object call() throws Signal {
					return delegate.removeObject(key);
				}
			}));
		} catch (Signal e) {
			throw new RuntimeException(e.getCause());
		}
	}

	@Override
	public void clear() {
		try {
			AsyncUtils.execute(new Task<Void>(new Callable<Void>() {
				@Override
				public Void call() throws Signal {
					delegate.clear();
					return null;
				}
			}));
		} catch (Signal e) {
			throw new RuntimeException(e.getCause());
		}
	}

	@Override
	public int getSize() {
		try {
			return AsyncUtils.execute(new Task<Integer>(new Callable<Integer>() {
				@Override
				public Integer call() throws Signal {
					return delegate.getSize();
				}
			}));
		} catch (Signal e) {
			throw new RuntimeException(e.getCause());
		}
	}

	@Override
	public ReadWriteLock getReadWriteLock() {
		return null;
	}
}
