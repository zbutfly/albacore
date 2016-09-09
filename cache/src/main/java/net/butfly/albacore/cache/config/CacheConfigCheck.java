package net.butfly.albacore.cache.config;

import net.butfly.albacore.cache.utils.control.CacheContant;

public class CacheConfigCheck {
	private CacheConfigCheck() {
		// TODO Auto-generated constructor stub
	}

	public static boolean check(CacheConfig[] cacheConfigs) {
		if (cacheConfigs == null || cacheConfigs.length <= 0) { return false; }
		if (cacheConfigs.length != 2) { return false; }
		int mainServerCount = 0;
		int standbyServerCount = 0;
		for (CacheConfig cacheConfig : cacheConfigs) {
			if (cacheConfig.getServiceType() == CacheContant.MAIN_CACHE_SERVER) {
				mainServerCount++;
				if (cacheConfig.getIsBatch() || !cacheConfig.getIsInit()) { return false; }
			} else {
				standbyServerCount++;
			}
		}
		if (mainServerCount != 1 || standbyServerCount != 1) { return false; }
		return true;
	}
}
