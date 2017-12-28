package net.butfly.albacore.lambda;

import java.lang.reflect.Proxy;

@SuppressWarnings("unchecked")
public abstract class InvocationHandler implements java.lang.reflect.InvocationHandler {
	public <T> T proxy(Class<? super T>... interfaces) {
		return (T) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), interfaces, this);
	}

	public <T> T proxy(Class<? super T> intf) {
		return (T) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class<?>[] { intf }, this);
	}

	public static <T> T proxy(InvocationHandler handler, Class<? super T>... interfaces) {
		return handler.proxy(interfaces);
	}

	public static <T> T proxy(InvocationHandler handler, Class<? super T> intf) {
		return handler.proxy(intf);
	}
}
