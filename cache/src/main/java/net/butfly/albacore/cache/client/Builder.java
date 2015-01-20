package net.butfly.albacore.cache.client;

import net.butfly.albacore.cache.config.CacheConfig;
import net.butfly.albacore.support.Bean;
import net.rubyeye.xmemcached.MemcachedClientBuilder;

public class Builder extends Bean<CacheConfig> {
	private static final long serialVersionUID = -3326780549732421226L;
	private MemcachedClientBuilder builder;
	private MemCacheClient client;
	private Integer serviceMark;
	private Integer serviceType;
	private String serviceContent;

	public String getServiceContent() {
		return serviceContent;
	}

	public void setServiceContent(String serviceContent) {
		this.serviceContent = serviceContent;
	}

	public MemCacheClient getClient() {
		return client;
	}

	public void setClient(MemCacheClient client) {
		this.client = client;
	}

	public MemcachedClientBuilder getBuilder() {
		return builder;
	}

	public void setBuilder(MemcachedClientBuilder builder) {
		this.builder = builder;
	}

	public Integer getServiceMark() {
		return serviceMark;
	}

	public void setServiceMark(Integer serviceMark) {
		this.serviceMark = serviceMark;
	}

	public Integer getServiceType() {
		return serviceType;
	}

	public void setServiceType(Integer serviceType) {
		this.serviceType = serviceType;
	}
}
