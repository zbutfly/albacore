package net.butfly.albacore.lambda;

import java.io.Serializable;

@FunctionalInterface
public interface Func0<R> extends Serializable {
	R call();
}
