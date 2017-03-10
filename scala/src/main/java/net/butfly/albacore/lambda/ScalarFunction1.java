package net.butfly.albacore.lambda;

import java.io.Serializable;
import java.util.function.Function;

public final class ScalarFunction1<T, R> extends scala.runtime.AbstractFunction1<T, R> implements Serializable, Function<T, R>,
		scala.Function1<T, R> {
	private static final long serialVersionUID = 1729009288332589663L;
	private final Function<T, R> c;

	public ScalarFunction1(Function<T, R> c) {
		this.c = c;
	}

	@Override
	public R apply(T arg0) {
		return c.apply(arg0);
	}
}