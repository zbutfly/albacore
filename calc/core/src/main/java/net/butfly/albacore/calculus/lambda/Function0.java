package net.butfly.albacore.calculus.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface Function0<R> extends Serializable {
	R call();
}
