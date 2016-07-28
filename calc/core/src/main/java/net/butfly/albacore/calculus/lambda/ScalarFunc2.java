package net.butfly.albacore.calculus.lambda;

import java.io.Serializable;

import scala.runtime.AbstractFunction2;

public abstract class ScalarFunc2<T1, T2, R> extends AbstractFunction2<T1, T2, R> implements Serializable {
	private static final long serialVersionUID = 1105560289309167475L;
}