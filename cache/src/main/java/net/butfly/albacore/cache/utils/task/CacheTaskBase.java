package net.butfly.albacore.cache.utils.task;

import net.butfly.albacore.cache.config.CacheConfig;
import net.butfly.albacore.cache.config.CacheConfigCheck;
import net.butfly.albacore.cache.config.CacheConfigService;
import net.butfly.albacore.exception.BusinessException;
import net.butfly.albacore.facade.FacadeBase;

public abstract class CacheTaskBase extends FacadeBase implements CacheTask {
	/**
	 * serialVersionUID:TODO（用一句话描述这个变量表示什么）
	 * 
	 * @since Ver 1.1
	 */
	private static final long serialVersionUID = 1L;
	protected CacheConfigService cacheConfigService;

	public CacheConfigService getCacheConfigService() {
		return cacheConfigService;
	}

	public void setCacheConfigService(CacheConfigService cacheConfigService) {
		this.cacheConfigService = cacheConfigService;
	}

	protected boolean check(CacheConfig cacheConfig) throws BusinessException {
		/**
		 * 对变化之后的缓存配置进行校验
		 */
		CacheConfig[] cacheConfigs = this.cacheConfigService.loadConfig();
		for (CacheConfig cacheConfig_ : cacheConfigs) {
			if (cacheConfig_.getServiceMark() == cacheConfig.getServiceMark()) {
				cacheConfig_.copy(cacheConfig);
			}
		}
		return CacheConfigCheck.check(cacheConfigs);
	}
}
