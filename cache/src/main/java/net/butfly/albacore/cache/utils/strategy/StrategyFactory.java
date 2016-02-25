package net.butfly.albacore.cache.utils.strategy;

import java.util.Map;

import net.butfly.albacore.cache.config.CacheConfigManager;
import net.butfly.albacore.exception.SystemException;

public class StrategyFactory {
	private StrategyFactory() {}

	private static final Map<String, ICacheStrategy> strategyCache = CacheConfigManager.getStrategys();

	public static ICacheStrategy getStrategy(String strategyId) {
		ICacheStrategy strategy = strategyCache.get(strategyId);
		if (null == strategy) { throw new SystemException("SYS_121", "the strategy[" + strategyId + "] not defined in cache.xml"); }
		return strategy;
	}
}
