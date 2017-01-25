package net.butfly.albacore.lambda;

import java.io.Serializable;
import java.util.function.BiFunction;

import scala.runtime.AbstractFunction2;

public final class ScalarFunction2<T1, T2, R> extends AbstractFunction2<T1, T2, R> implements Serializable {
	private static final long serialVersionUID = 1105560289309167475L;
	private final BiFunction<T1, T2, R> conv;

	public ScalarFunction2(BiFunction<T1, T2, R> conv) {
		super();
		this.conv = conv;
	}

	@Override
	public R apply(T1 v1, T2 v2) {
		return conv.apply(v1, v2);
	}
}