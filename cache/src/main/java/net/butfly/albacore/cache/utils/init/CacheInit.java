package net.butfly.albacore.cache.utils.init;

import java.io.IOException;
import java.util.Properties;

import net.butfly.albacore.cache.client.MemCachePond;
import net.butfly.albacore.cache.config.CacheConfig;
import net.butfly.albacore.cache.config.CacheConfigBase;
import net.butfly.albacore.cache.config.CacheConfigCheck;
import net.butfly.albacore.cache.config.CacheConfigService;
import net.butfly.albacore.cache.utils.control.CacheContant;
import net.butfly.albacore.cache.utils.control.CacheControl;
import net.butfly.albacore.exception.BusinessException;
import net.butfly.albacore.exception.SystemException;
import net.butfly.albacore.helper.AsyncHelper;
import net.butfly.albacore.logger.Logger;
import net.butfly.albacore.logger.LoggerFactory;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

public class CacheInit {
	private CacheConfigService cacheConfigService;
	private AsyncHelper asyncHelper;
	public static boolean cacheTaskWait = false;

	public AsyncHelper getAsyncHelper() {
		return asyncHelper;
	}

	public void setAsyncHelper(AsyncHelper asyncHelper) {
		this.asyncHelper = asyncHelper;
	}

	public void setCacheConfigService(CacheConfigService cacheConfigService) {
		this.cacheConfigService = cacheConfigService;
	}

	public void init() {
		CacheConfig[] cacheConfigs;
		try {
			cacheConfigs = cacheConfigService.loadConfig();
			if (!CacheConfigCheck.check(cacheConfigs)) { throw new SystemException("SYS_119",
					"the cache config is error  ,can't init with this config! "); }
			MemCachePond.getInstance().init(cacheConfigs);
			Thread heartBeat = new CacheTask("cacheInit");
			heartBeat.start();
		} catch (Exception e1) {
			throw new SystemException("SYS_119", "the cache config is error  ,can't init with this config!  case by \n"
					+ e1.getMessage());
		}
	}

	public static void initFile(String filePath) {
		Properties pro = new Properties();
		Resource rs = new ClassPathResource(filePath);
		try {
			pro.load(rs.getInputStream());
		} catch (IOException e1) {
			e1.printStackTrace();
			throw new SystemException("SYS_119", "the cache.properties can't load!!!");
		}
		CacheConfig[] cacheConfigs = new CacheConfig[2];
		CacheConfig mainConfig = new CacheConfig();
		mainConfig.setServiceContent(pro.getProperty("mainServer"));
		mainConfig.setServiceType(CacheContant.MAIN_CACHE_SERVER);
		mainConfig.setConnectionPool(Integer.valueOf(pro.getProperty("mainConn")));
		CacheConfig standbyConfig = new CacheConfig();
		standbyConfig.setServiceContent(pro.getProperty("standByServer"));
		standbyConfig.setServiceType(CacheContant.STANDBY_CACHE_SERVER);
		standbyConfig.setConnectionPool(Integer.valueOf(pro.getProperty("standByConn")));
		cacheConfigs[0] = mainConfig;
		cacheConfigs[1] = standbyConfig;
		MemCachePond.getInstance().init(cacheConfigs);
	}
}

class CacheTask extends Thread {
	public static Logger log = LoggerFactory.getLogger(CacheTask.class);

	public CacheTask(String name) {
		super(name);
	}

	@Override
	public void run() {
		while (true) {
			try {
				while (CacheInit.cacheTaskWait) {}
				CacheConfig[] listCacheConfig = CacheConfigBase.getCacheConfigService().loadConfig();
				CacheConfig usedCacheServer = null; // 数据库中主用缓存服务器配置
				CacheConfig standbyCacheServer = null; // 数据库中备用用缓存服务器配置
				for (CacheConfig cacheConfig : listCacheConfig) {
					if (cacheConfig.getServiceType() == CacheContant.MAIN_CACHE_SERVER) {
						usedCacheServer = cacheConfig;
					} else {
						standbyCacheServer = cacheConfig;
					}
				}
				/**
				 * 监控当前节点主备状态和服务器上的主备状态是否一致,如果不一致则进行主备切换。
				 */
				int mainMark = MemCachePond.getInstance().getServiceMark(CacheContant.MAIN_CACHE_SERVER);
				log.debug(" main : |Pond: " + mainMark + "| Db： " + usedCacheServer.getServiceMark());
				if (usedCacheServer.getServiceMark() == mainMark) {
					MemCachePond.getInstance().switchServer();
				}
				/**
				 * 将数据库中主用缓存配置的酒店和机票控制更新每个EBOX中
				 */
				CacheControl.setCacheOn(usedCacheServer.getHotelControl() == 3 ? false : true);
				CacheControl.setCacheUseOn(usedCacheServer.getHotelControl() == 1 ? true : false);
				CacheControl.setCacheOnFlight(usedCacheServer.getFlightControl() == 3 ? false : true);
				CacheControl.setCacheUseOnFlight(usedCacheServer.getFlightControl() == 1 ? true : false);
				/**
				 * 监控*备用*缓存服务器中服务器列表配置是否更改过。如果更改过重新初始化MemCachePond
				 */
				String standbyServiceContent = MemCachePond.getInstance().getServiceContent(CacheContant.STANDBY_CACHE_SERVER);
				if (!standbyServiceContent.trim().equals(standbyCacheServer.getServiceContent().trim())) {
					MemCachePond.getInstance().refresh(standbyCacheServer);
				}
				try {
					Thread.sleep(1000 * 60);
				} catch (InterruptedException e) {}
			} catch (BusinessException e1) {
				log.error(" the heartbeat thread is error with :" + e1.getMessage());
			}
		}
	}
}
