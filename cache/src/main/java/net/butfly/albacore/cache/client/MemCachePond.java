package net.butfly.albacore.cache.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.butfly.albacore.cache.config.CacheConfig;
import net.butfly.albacore.cache.utils.control.CacheContant;
import net.butfly.albacore.exception.SystemException;
import net.rubyeye.xmemcached.MemcachedClientBuilder;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;
import net.rubyeye.xmemcached.command.TextCommandFactory;
import net.rubyeye.xmemcached.impl.KetamaMemcachedSessionLocator;
import net.rubyeye.xmemcached.transcoders.SerializingTranscoder;
import net.rubyeye.xmemcached.utils.AddrUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.yanf4j.core.impl.StandardSocketOption;

public class MemCachePond {
	private static MemCachePond memCachePond = new MemCachePond();
	private List<Builder> builders;

	public static MemCachePond getInstance() {
		return memCachePond;
	}

	public int getServiceMark(int serviceType) {
		int mainMark = 0;
		for (Builder builder : builders) {
			if (builder.getServiceType().equals(serviceType)) {
				mainMark = builder.getServiceMark();
			}
		}
		return mainMark;
	}

	public String getServiceContent(int serviceType) {
		String serviceContent = "";
		for (Builder builder : builders) {
			if (builder.getServiceType().equals(serviceType)) {
				serviceContent = builder.getServiceContent();
			}
		}
		return serviceContent;
	}

	/**
	 * get memcacheClient by select ploy get one client with stats is starting
	 * 
	 * @return MemcachedClient
	 * @since CodingExample Ver(编码范例查看) 1.1
	 * @auto xhb
	 */
	public MemCacheClient getClient() {
		if (builders == null
				|| builders.size() <= 0) { throw new SystemException("SYS_119", "must init cacheServer before use memcacheClinet"); }
		MemCacheClient client = null;
		for (Builder builder : builders) {
			if (builder.getServiceType() == CacheContant.MAIN_CACHE_SERVER) {
				client = builder.getClient();
			}
		}
		return client;
	}

	public MemCacheClient getClient(int serviceType) {
		if (builders == null
				|| builders.size() <= 0) { throw new SystemException("SYS_119", "must init cacheServer before use memcacheClinet"); }
		MemCacheClient client = null;
		for (Builder builder : builders) {
			if (builder.getServiceType().intValue() == serviceType) {
				client = builder.getClient();
			}
		}
		if (client == null) { throw new SystemException("SYS_119", " not exist available server whith serviceType:" + serviceType); }
		return client;
	}

	/**
	 * this method only use in starting ;
	 * 
	 * @param cacheConfigs
	 * @throws IOException
	 * @since CodingExample Ver(编码范例查看) 1.1
	 * @auto xhb
	 */
	public void init(CacheConfig[] cacheConfigs) {
		int count = cacheConfigs.length;
		builders = new ArrayList<Builder>();
		for (int i = 0; i < count; i++) {
			Builder build = new Builder();
			CacheConfig cacheconfig = cacheConfigs[i];
			build.copy(cacheconfig);
			MemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(cacheconfig.getServiceContent()));
			builder.setConnectionPoolSize(cacheconfig.getConnectionPool());
			builder.setCommandFactory(new TextCommandFactory());
			builder.setSessionLocator(new KetamaMemcachedSessionLocator());
			builder.setTranscoder(new SerializingTranscoder());
			builder.setFailureMode(true);
			builder.setSocketOption(StandardSocketOption.SO_RCVBUF, 32 * 1024); // 设置接收缓存区为32K，默认16K
			builder.setSocketOption(StandardSocketOption.SO_SNDBUF, 16 * 1024); // 设置发送缓冲区为16K，默认为8K
			builder.getConfiguration().setCheckSessionTimeoutInterval(10 * 60 * 1000L);
			builder.getConfiguration().setSessionIdleTimeout(10 * 60 * 1000L);
			MemCacheClientImpl client = new MemCacheClientImpl();
			try {
				client.setMemcachedClientExt(builder.build());
			} catch (IOException e) {
				throw new SystemException("SYS_119", "the cache config is error  ,can't init with this config! ");
			}
			build.setBuilder(builder);
			build.setClient(client);
			builders.add(build);
		}
	}

	/**
	 * this method only use in running
	 * 
	 * @param cacheConfig
	 * @since CodingExample Ver(编码范例查看) 1.1
	 * @auto xhb
	 */
	public void refresh(CacheConfig cacheConfig) {
		Builder build = new Builder();
		build.copy(cacheConfig);
		MemcachedClientBuilder builder = new XMemcachedClientBuilder(AddrUtil.getAddresses(cacheConfig.getServiceContent()));
		builder.setConnectionPoolSize(cacheConfig.getConnectionPool());
		builder.setCommandFactory(new TextCommandFactory());
		builder.setSessionLocator(new KetamaMemcachedSessionLocator());
		builder.setTranscoder(new SerializingTranscoder());
		builder.setFailureMode(true);
		builder.setSocketOption(StandardSocketOption.SO_RCVBUF, 32 * 1024); // 设置接收缓存区为32K，默认16K
		builder.setSocketOption(StandardSocketOption.SO_SNDBUF, 16 * 1024); // 设置发送缓冲区为16K，默认为8K
		builder.getConfiguration().setCheckSessionTimeoutInterval(10 * 60 * 1000L);
		builder.getConfiguration().setSessionIdleTimeout(10 * 60 * 1000L);
		MemCacheClientImpl client = new MemCacheClientImpl();
		try {
			client.setMemcachedClientExt(builder.build());
		} catch (IOException e) {
			throw new SystemException("SYS_119", "the cache config is error  ,can't refresh with this config! ");
		}
		build.setBuilder(builder);
		build.setClient(client);
		for (int i = 0; i < builders.size(); i++) {
			if (cacheConfig.getServiceType() == builders.get(i).getServiceType()) {
				builders.remove(i);
				builders.add(build);
				break;
			}
		}
	}

	public void switchServer() {
		if (builders == null
				|| builders.size() <= 0) { throw new SystemException("SYS_119", "must init cacheServer before use memcacheClinet"); }
		Builder mainServer = null;
		Builder standbyServer = null;
		for (Builder builder : builders) {
			if (builder.getServiceType().equals(CacheContant.MAIN_CACHE_SERVER)) {
				mainServer = builder;
			} else {
				standbyServer = builder;
			}
		}
		if (null != mainServer && null != standbyServer) {
			mainServer.setServiceType(CacheContant.STANDBY_CACHE_SERVER);
			standbyServer.setServiceType(CacheContant.MAIN_CACHE_SERVER);
		} else {
			log.error(" the config is error whith builders !!! ");
		}
	}

	private Logger log = LoggerFactory.getLogger(this.getClass());
}
