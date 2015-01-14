package net.butfly.albacore.cache.utils.impl;

import java.util.concurrent.TimeoutException;

import net.butfly.albacore.cache.client.MemCachePond;
import net.butfly.albacore.cache.config.CacheConfigBase;
import net.butfly.albacore.cache.utils.ICacheHelper;
import net.butfly.albacore.cache.utils.Key;
import net.butfly.albacore.cache.utils.control.CacheContant;
import net.butfly.albacore.cache.utils.strategy.ICacheStrategy;
import net.butfly.albacore.cache.utils.strategy.StrategyFactory;
import net.butfly.albacore.exception.SystemException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseCacheImpl implements ICacheHelper {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	protected ICacheStrategy strategy;

	public void setStrategy(String strategyId) {
		this.strategy = StrategyFactory.getStrategy(strategyId);
	}

	public Object get(Key key) {
		String key_str;
		key_str = strategy.getKeyGenerator().getKey(key);
		Object result = null;
		try {
			result = strategy.getMemCacheClient().get(key_str);
		} catch (SystemException e) {
			logger.error("some error with the main cache server :" + e.getMessage());
			CacheConfigBase.notifyError(CacheContant.MAIN_CACHE_SERVER);
			try {
				result = strategy.getMemCacheClient(CacheContant.STANDBY_CACHE_SERVER).get(key_str);
			} catch (SystemException e_) {
				logger.error("some error with the standby cache server :" + e_.getMessage());
				CacheConfigBase.notifyError(CacheContant.STANDBY_CACHE_SERVER);
				throw new SystemException("SYS_121", "some error with main and standby cache server ,this query use db !!!");
			} catch (TimeoutException e1_) {
				throw new SystemException("SYS_121", e1_.getMessage());
			}
		} catch (TimeoutException e1) {
			throw new SystemException("SYS_121", e1.getMessage());
		}
		return result;
	}

	public void set(Key key, Object value) {
		String key_str = strategy.getKeyGenerator().getKey(key);
		try {
			strategy.getMemCacheClient().set(key_str, strategy.getExpiration(), value);
		} catch (SystemException e) {
			logger.error("some error with the main cace server :" + e.getMessage());
			CacheConfigBase.notifyError(CacheContant.MAIN_CACHE_SERVER);
			throw new SystemException("SYS_121", e);
		}
	}

	public boolean invalidate(Key key) {
		String key_str = strategy.getKeyGenerator().getKey(key);
		strategy.getMemCacheClient().delete(key_str);
		return true;
	}

	public void set(Key key, Object value, int serviceType) {
		String key_str = strategy.getKeyGenerator().getKey(key);
		try {
			strategy.getMemCacheClient(serviceType).set(key_str, strategy.getExpiration(), value);
		} catch (SystemException e) {
			logger.error("some error with the " + (serviceType == CacheContant.MAIN_CACHE_SERVER ? "main" : "standby")
					+ " cace server :" + e.getMessage());
			CacheConfigBase.notifyError(MemCachePond.getInstance().getServiceMark(serviceType));
			throw new SystemException("SYS_121", e);
		}
	}

	public boolean invalidate(Key key, int serviceType) {
		String key_str = strategy.getKeyGenerator().getKey(key);
		strategy.getMemCacheClient(serviceType).delete(key_str);
		return true;
	}
}
