package net.butfly.albacore.utils.aop;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import net.butfly.albacore.utils.logger.Logger;

public class AsyncInterceptor implements MethodInterceptor {
	private static final Logger logger = Logger.getLogger(AsyncInterceptor.class);

	@Override
	public Object invoke(MethodInvocation invocation) throws Throwable {
		logger.trace("AOP async intecetping: " + invocation + "]");
		Object result = invocation.proceed();
		logger.trace("AOP async intecetped: " + invocation + "]");
		return result;
	}
}
