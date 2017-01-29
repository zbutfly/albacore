package net.butfly.albacore.lambda;

import java.lang.reflect.Proxy;

@SuppressWarnings("unchecked")
@FunctionalInterface
public interface InvocationHandler extends java.lang.reflect.InvocationHandler {
	default <T> T proxy(Class<? super T>... interfaces) {
		return (T) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), interfaces, this);
	}

	default <T> T proxy(Class<? super T> intf) {
		return (T) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class<?>[] { intf }, this);
	}

	static <T> T proxy(InvocationHandler handler, Class<? super T>... interfaces) {
		return handler.proxy(interfaces);
	}

	static <T> T proxy(InvocationHandler handler, Class<? super T> intf) {
		return handler.proxy(intf);
	}
}
