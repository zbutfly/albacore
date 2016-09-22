package net.butfly.albacore.lambda;

import java.io.Serializable;

import scala.runtime.AbstractFunction1;

public abstract class ScalarFunction1<T, R> extends AbstractFunction1<T, R> implements Serializable {
	private static final long serialVersionUID = 1729009288332589663L;
}