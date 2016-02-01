package net.butfly.albacore.cache.utils.strategy;

import net.butfly.albacore.cache.client.MemCacheClient;
import net.butfly.albacore.cache.client.MemCachePond;
import net.butfly.albacore.cache.utils.strategy.keygenerate.IKeyGenerator;

public class ICacheStrategy {
	public ICacheStrategy(IKeyGenerator keyGenerator, int expiration) {
		this.expiration = expiration;
		this.keyGenerator = keyGenerator;
	}

	private IKeyGenerator keyGenerator;
	private int expiration;

	public IKeyGenerator getKeyGenerator() {
		return keyGenerator;
	}

	public int getExpiration() {
		return expiration;
	}

	public MemCacheClient getMemCacheClient() {
		return MemCachePond.getInstance().getClient();
	}

	public MemCacheClient getMemCacheClient(int serviceType) {
		return MemCachePond.getInstance().getClient(serviceType);
	}
}
