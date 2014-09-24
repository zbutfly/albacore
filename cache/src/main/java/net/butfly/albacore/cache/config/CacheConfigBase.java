package net.butfly.albacore.cache.config;

import net.butfly.albacore.cache.client.MemCachePond;
import net.butfly.albacore.exception.BusinessException;
import net.butfly.albacore.exception.SystemException;

public class CacheConfigBase {
	protected static CacheConfigService cacheConfigService;

	public void setCacheConfigService(CacheConfigService cacheConfigService_) {
		cacheConfigService = cacheConfigService_;
	}

	public static CacheConfigService getCacheConfigService() {
		return cacheConfigService;
	}

	public static void notifyError(int serviceType) {
		if (cacheConfigService != null) {
			try {
				cacheConfigService.notifyError(MemCachePond.getInstance().getServiceMark(serviceType));
			} catch (BusinessException e) {
				throw new SystemException("", e);
			}
		}
	}
}
