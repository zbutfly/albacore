package net.butfly.albacore.calculus.datasource;

import java.io.Serializable;

import net.butfly.albacore.calculus.factor.Factor.Type;

public abstract class Detail implements Serializable {
	private static final long serialVersionUID = 1900035964021610093L;
	public Type type;

	public Detail(Type type) {
		this.type = type;
	}

	abstract public String toString();
}
