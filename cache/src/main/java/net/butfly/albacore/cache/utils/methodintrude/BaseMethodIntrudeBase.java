package net.butfly.albacore.cache.utils.methodintrude;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import net.butfly.albacore.logger.Logger;
import net.butfly.albacore.logger.LoggerFactory;

public class BaseMethodIntrudeBase {
	protected static final Logger logger = LoggerFactory.getLogger(BaseMethodIntrudeBase.class);
	public Set<Method> useMap = new HashSet<Method>();

	public Set<Method> getUseMethods() {
		return useMap;
	}

	public void setUseMethod(Class<?> cls, String methodName, Class<?>... parameterTypes) {
		Method method = getMethod(cls, methodName, parameterTypes);
		if (null != method) {
			useMap.add(method);
		}
	}

	public Method getMethod(Class<?> cls, String methodName, Class<?>... parameterTypes) {
		try {
			return cls.getMethod(methodName, parameterTypes);
		} catch (SecurityException e) {
			logger.warn("baseMethod lost : class:" + cls + " method:" + methodName);
			return null;
		} catch (NoSuchMethodException e) {
			logger.warn("baseMethod lost : class:" + cls + " method:" + methodName);
			return null;
		}
	}
}
