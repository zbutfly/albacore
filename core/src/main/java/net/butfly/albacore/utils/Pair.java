package net.butfly.albacore.utils;

import java.io.Serializable;

public class Pair<T1, T2> implements Serializable {
	private static final long serialVersionUID = 6995675769216721583L;
	private T1 v1;
	private T2 v2;

	public Pair() {
		super();
	}

	public Pair(T1 v1, T2 v2) {
		this();
		this.v1 = v1;
		this.v2 = v2;
	}

	public T1 value1() {
		return v1;
	}

	public T2 value2() {
		return v2;
	}

	public void value1(T1 v1) {
		this.v1 = v1;
	}

	public void value2(T2 v2) {
		this.v2 = v2;
	}

	@Override
	public String toString() {
		return "[" + v1 + "," + v2 + "]";
	}
}
