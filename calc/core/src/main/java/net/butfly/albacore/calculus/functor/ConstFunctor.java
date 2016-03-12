package net.butfly.albacore.calculus.functor;

import javax.transaction.NotSupportedException;

import net.butfly.albacore.calculus.Functor;

public abstract class ConstFunctor<V> implements Functor<ConstFunctor<V>> {
	private static final long serialVersionUID = 1L;
	public V value;

	protected ConstFunctor() {}

	public ConstFunctor(String str) {
		throw new RuntimeException(new NotSupportedException());
	}

	public ConstFunctor(V value) {
		this.value = value;
	}
}
