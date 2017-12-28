//package net.butfly.albacore.lambda;
//
//import java.io.Serializable;
//import java.util.Objects;
//
//@FunctionalInterface
//public interface Consumer<T> extends Serializable, java.util.function.Consumer<T> {
//	@Override
//	void accept(T t);
//
//	@Override
//	default Consumer<T> andThen(java.util.function.Consumer<? super T> after) {
//		Objects.requireNonNull(after);
//		return (T t) -> {
//			accept(t);
//			after.accept(t);
//		};
//	}
//}
