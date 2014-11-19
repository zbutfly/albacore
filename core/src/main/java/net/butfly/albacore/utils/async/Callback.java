package net.butfly.albacore.utils.async;

@FunctionalInterface
public interface Callback<R> {
	void callback(final R result) throws Signal;
}
