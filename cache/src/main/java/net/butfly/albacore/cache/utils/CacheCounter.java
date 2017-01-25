package net.butfly.albacore.cache.utils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.cache.client.MemCacheClient;
import net.butfly.albacore.cache.client.MemCachePond;
import net.butfly.albacore.cache.utils.control.CacheContant;
import net.butfly.albacore.cache.utils.strategy.keygenerate.StringKeyGenerator;

public class CacheCounter {
	public static List<Map<InetSocketAddress, Map<String, String>>> getStats() {
		List<Map<InetSocketAddress, Map<String, String>>> list = new ArrayList<Map<InetSocketAddress, Map<String, String>>>();
		MemCacheClient mainClient = MemCachePond.getInstance().getClient(CacheContant.MAIN_CACHE_SERVER);
		MemCacheClient standbyClient = MemCachePond.getInstance().getClient(CacheContant.STANDBY_CACHE_SERVER);
		list.add(mainClient.getStats());
		list.add(standbyClient.getStats());
		return list;
	}

	/**
	 * the result , 1: main is delete,standby is delete, 0: main is delete
	 * ,standby is undelete -1: main and standby is undelete, -2: standy is
	 * delete, main is undelete
	 * 
	 * @param key
	 * @return
	 * @since CodingExample Ver(编码范例查看) 1.1
	 * @auto xhb
	 */
	public static int delete(String key) {
		MemCacheClient mainClient = MemCachePond.getInstance().getClient(CacheContant.MAIN_CACHE_SERVER);
		MemCacheClient standbyClient = MemCachePond.getInstance().getClient(CacheContant.STANDBY_CACHE_SERVER);
		int result = -1;
		String key_ = StringKeyGenerator.instance.getKey(new Key(key));
		boolean main = mainClient.deleteWhithResult(key_);
		if (main) result = 0;
		if (standbyClient.deleteWhithResult(key_)) result = main ? 1 : -2;
		return result;
	}
}
