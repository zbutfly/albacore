package net.butfly.albacore.utils.async;

//@FunctionalInterface for JDK 8
public interface Callable<R> {
	R call() throws Signal;
}
