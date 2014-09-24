package net.butfly.albacore.cache.utils;

import java.util.Map;
import java.util.WeakHashMap;

import net.butfly.albacore.cache.config.CacheConfigManager;
import net.butfly.albacore.cache.utils.impl.BaseCacheImpl;
import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.logger.Logger;
import net.butfly.albacore.logger.LoggerFactory;

public class CacheFactory {
	private final static Logger logger = LoggerFactory.getLogger(CacheFactory.class);
	private static final Map<String, ICacheHelper> cacheHelperCache = new WeakHashMap<String, ICacheHelper>();

	private CacheFactory() {}

	public static ICacheHelper getCacheHelper(String configId) throws SystemException {
		ICacheHelper iCacheHelper = cacheHelperCache.get(configId);
		if (null == iCacheHelper) {
			String type = CacheConfigManager.getCacheType(configId);
			String strategyId = CacheConfigManager.getStrategyId(configId);
			if (null == type) { throw new SystemException("SYS_121", "the config : [" + configId + "] not defined in cache.xml"
					+ " or the attribute type not defined in config: [" + configId + "]"); }
			if (null == strategyId) { throw new SystemException("SYS_121", "the config : [" + configId
					+ "] not defined in cache.xml" + " or the attribute strategyId not defined in config: [" + configId + "]"); }
			if (CacheConfigManager.CACHE_TYPE_BASE.equals(type)) {
				BaseCacheImpl baseImpl = new BaseCacheImpl();
				baseImpl.setStrategy(strategyId);
				cacheHelperCache.put(configId, baseImpl);
				iCacheHelper = baseImpl;
			} else {
				throw new SystemException("SYS_121", "the cacheType[" + type + "] is not be supported");
			}
			logger.debug(" init ICacheHelper  with type: [" + type + "] and strategyId: [" + strategyId + "] successed !!!");
		}
		return iCacheHelper;
	}
}
