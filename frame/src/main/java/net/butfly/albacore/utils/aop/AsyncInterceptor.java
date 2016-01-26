package net.butfly.albacore.utils.aop;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncInterceptor implements MethodInterceptor {
	private static final Logger logger = LoggerFactory.getLogger(AsyncInterceptor.class);

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		logger.trace("AOP async intecetping: " + invocation + "]");
		Object result = invocation.proceed();
		logger.trace("AOP async intecetped: " + invocation + "]");
		return result;
	}
}
