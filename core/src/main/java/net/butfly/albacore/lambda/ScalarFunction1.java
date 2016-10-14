package net.butfly.albacore.lambda;

import java.io.Serializable;

import scala.runtime.AbstractFunction1;

public final class ScalarFunction1<T, R> extends AbstractFunction1<T, R> implements Serializable, Converter<T, R> {
	private static final long serialVersionUID = 1729009288332589663L;
	private final Converter<T, R> c;

	public ScalarFunction1(Converter<T, R> c) {
		this.c = c;
	}

	@Override
	public R apply(T arg0) {
		return c.apply(arg0);
	}
}