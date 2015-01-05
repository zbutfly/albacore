package net.butfly.albacore.utils.async;

//@FunctionalInterface for JDK 8
public interface Callback<R> {
	void callback(final R result) throws Signal;
}
