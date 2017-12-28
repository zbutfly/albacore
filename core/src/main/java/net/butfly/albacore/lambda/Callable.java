//package net.butfly.albacore.lambda;
//
//import java.io.Serializable;
//
//@FunctionalInterface
//public interface Callable<V> extends java.util.concurrent.Callable<V>, Supplier<V>, Serializable {
//	/**
//	 * Computes a result, or throws an exception if unable to do so.
//	 *
//	 * @return computed result
//	 * @throws Exception
//	 *             if unable to compute a result
//	 */
//	@Override
//	V call();
//
//	@Override
//	default V get() {
//		try {
//			return call();
//		} catch (RuntimeException e) {
//			throw e;
//		} catch (Exception e) {
//			throw new RuntimeException(e);
//		}
//	}
//}
