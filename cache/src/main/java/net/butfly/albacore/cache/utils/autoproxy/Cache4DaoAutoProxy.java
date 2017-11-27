package net.butfly.albacore.cache.utils.autoproxy;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import net.butfly.albacore.cache.config.CacheConfigManager;
import net.butfly.albacore.cache.utils.CacheFactory;
import net.butfly.albacore.cache.utils.ICacheHelper;
import net.butfly.albacore.cache.utils.Key;
import net.butfly.albacore.cache.utils.control.CacheControl;
import net.butfly.albacore.exception.SystemException;

/**
 * 
 * 项目名称：ebase 类名称：Cache4DaoAutoProxy 类描述： 缓存自动拦截 创建人：xhb 创建时间：2010-11-10
 * 上午10:28:18 修改人：xhb 修改时间：2010-11-10 上午10:28:18
 * 
 * @version *
 */
public class Cache4DaoAutoProxy implements MethodInterceptor {
	public static final String HOTEL_NAMESPACE = ".hotel";
	public static final String FLIGHT_NAMESPACE = ".flight";

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		if (invocation.getMethod().toString().contains(HOTEL_NAMESPACE) && !CacheControl.isCacheOn()) return invocation.proceed();
		if (invocation.getMethod().toString().contains(FLIGHT_NAMESPACE)
				&& !CacheControl.isCacheOnFlight()) return invocation.proceed();
		if (!CacheConfigManager.isAutoProxy(invocation.getMethod())) return invocation.proceed();
		String configId = CacheConfigManager.getConfigsByMethod(invocation.getMethod());
		if (invocation.getMethod().toString().contains(HOTEL_NAMESPACE) && !CacheControl.isCacheUseOn()) return invocation.proceed();
		if (invocation.getMethod().toString().contains(FLIGHT_NAMESPACE)
				&& !CacheControl.isCacheUseOnFlight()) return invocation.proceed();
		String type = CacheConfigManager.getCacheType(configId);
		if (CacheConfigManager.CACHE_TYPE_BASE.equals(type)) {
			return baseUse(invocation, configId);
		} else {
			return invocation.proceed();
		}
	}

	private Object baseUse(MethodInvocation invocation, String configId) throws Throwable {
		Object result = null;
		try {
			ICacheHelper icacheHelper = CacheFactory.getCacheHelper(configId);
			Key key = new Key(invocation.getMethod(), invocation.getArguments());
			result = icacheHelper.get(key);
			if (null == result) {
				result = invocation.proceed();
				if (null != result) {
					icacheHelper.set(key, result);
				}
			}
		} catch (SystemException e) {
			if (null == result) {
				result = invocation.proceed();
			}
		}
		return result;
	}
}
