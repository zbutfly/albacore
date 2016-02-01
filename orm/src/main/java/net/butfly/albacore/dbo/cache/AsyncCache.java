package net.butfly.albacore.dbo.cache;

import java.util.concurrent.locks.ReadWriteLock;

import net.butfly.albacore.utils.Reflections;
import net.butfly.albacore.utils.async.Task;

import org.apache.ibatis.cache.Cache;

public class AsyncCache implements Cache {
	private Class<? extends Cache> delegateClass;
	private Cache delegate;

	public AsyncCache(final String id) {
		try {
			this.delegate = Reflections.construct(this.delegateClass, id);
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
			new Task<Void>(new Task.Callable<Void>() {
				@Override
				public Void call() {
					delegate.putObject(key, value);
					return null;
				}
			}).execute();
		} catch (Exception e) {}
	}

	@Override
	public Object getObject(final Object key) {
		try {
			return new Task<Object>(new Task.Callable<Object>() {
				@Override
				public Object call() {
					return delegate.getObject(key);
				}
			}).execute();
		} catch (Exception e) {
			return null;
		}
	}

	@Override
	public Object removeObject(final Object key) {
		try {
			return new Task<Object>(new Task.Callable<Object>() {
				@Override
				public Object call() {
					return delegate.removeObject(key);
				}
			}).execute();
		} catch (Exception e) {
			throw new RuntimeException(e.getCause());
		}
	}

	@Override
	public void clear() {
		try {
			new Task<Void>(new Task.Callable<Void>() {
				@Override
				public Void call() {
					delegate.clear();
					return null;
				}
			}).execute();
		} catch (Exception e) {
			throw new RuntimeException(e.getCause());
		}
	}

	@Override
	public int getSize() {
		try {
			return new Task<Integer>(new Task.Callable<Integer>() {
				@Override
				public Integer call() {
					return delegate.getSize();
				}
			}).execute();
		} catch (Exception e) {
			throw new RuntimeException(e.getCause());
		}
	}

	@Override
	public ReadWriteLock getReadWriteLock() {
		return null;
	}
}
