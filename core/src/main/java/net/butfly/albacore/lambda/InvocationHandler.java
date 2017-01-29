package net.butfly.albacore.lambda;

import java.lang.reflect.Proxy;

@SuppressWarnings("unchecked")
@FunctionalInterface
public interface InvocationHandler extends java.lang.reflect.InvocationHandler {
	static <T> T proxy(InvocationHandler handler, Class<? super T>... interfaces) {
		return (T) Proxy.newProxyInstance(Object.class.getClassLoader(), interfaces, handler);
	}

	static <T> T proxy(InvocationHandler handler, Class<? super T> intf) {
		return (T) Proxy.newProxyInstance(Object.class.getClassLoader(), new Class<?>[] { intf }, handler);
	}
}
