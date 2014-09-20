package net.butfly.albacore.cache.client;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import net.butfly.albacore.exception.SystemException;
import net.rubyeye.xmemcached.CASOperation;
import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.exception.MemcachedException;

public class MemCacheClientImpl implements MemCacheClient {
	private MemcachedClient memcachedClientExt;

	public void setMemcachedClientExt(MemcachedClient memcachedClientExt) {
		this.memcachedClientExt = memcachedClientExt;
		this.memcachedClientExt.getTranscoder().setCompressionThreshold(1024 * 10); // 最小压缩阀值为
																					// 10K
		this.memcachedClientExt.setConnectTimeout(10 * 1000);
		this.memcachedClientExt.setHealSessionInterval(10 * 60 * 1000L);
		this.memcachedClientExt.setEnableHeartBeat(true);
	}

	public MemcachedClient getMemcachedClientExt() {
		return memcachedClientExt;
	}

	public void append(String s, Object obj) {
		try {
			memcachedClientExt.append(s, obj);
		} catch (TimeoutException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (MemcachedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		}
	}

	public void delete(String s) {
		try {
			memcachedClientExt.delete(s);
		} catch (TimeoutException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (MemcachedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		}
	}

	public boolean deleteWhithResult(String s) {
		try {
			return memcachedClientExt.delete(s);
		} catch (Exception e) {
			throw new SystemException("SYS_121", e);
		}
	}

	public Object get(String s) throws TimeoutException {
		Object value = null;
		try {
			value = memcachedClientExt.get(s);
		} catch (TimeoutException e) {
			e.printStackTrace();
			throw e;
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (MemcachedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		}
		return value;
	}

	public void prepend(String s, Object obj) {
		try {
			memcachedClientExt.prepend(s, obj);
		} catch (TimeoutException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (MemcachedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		}
	}

	public void replace(String s, int i, Object obj) {
		try {
			memcachedClientExt.replace(s, i, obj);
		} catch (TimeoutException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (MemcachedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		}
	}

	public void set(String s, int i, Object obj) {
		try {
			memcachedClientExt.set(s, i, obj, 10000);
		} catch (TimeoutException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (MemcachedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		}
	}

	public void casMax(String s, int i, Object obj) {
		try {
			memcachedClientExt.cas(s, i, new CASOperation<Object>() {
				public int getMaxTries() {
					return 10;
				}

				public Object getNewValue(long currentCAS, Object currentValue) {
					return currentValue;
				}
			});
		} catch (TimeoutException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (MemcachedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_122");
		}
	}

	public boolean cas(String s, int i, Object obj, long cas) {
		try {
			return memcachedClientExt.cas(s, i, obj, cas);
		} catch (TimeoutException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (MemcachedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		}
	}

	public long getCas(String s) {
		try {
			return memcachedClientExt.gets(s).getCas();
		} catch (TimeoutException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (MemcachedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		}
	}

	public Map<InetSocketAddress, Map<String, String>> getStats() {
		try {
			return memcachedClientExt.getStats();
		} catch (MemcachedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		} catch (TimeoutException e) {
			e.printStackTrace();
			throw new SystemException("SYS_121", e);
		}
	}
}
