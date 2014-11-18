package net.butfly.albacore.utils.async;

@FunctionalInterface
public interface Callable<R> {
	R call() throws Signal;
}
