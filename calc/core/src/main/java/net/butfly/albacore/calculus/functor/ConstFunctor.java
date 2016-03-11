package net.butfly.albacore.calculus.functor;

import net.butfly.albacore.calculus.Functor;
import net.butfly.albacore.exception.NotImplementedException;

public abstract class ConstFunctor<V> implements Functor<ConstFunctor<V>> {
	private static final long serialVersionUID = 1L;
	public V value;

	protected ConstFunctor() {}

	public ConstFunctor(String str) {
		throw new NotImplementedException();
	}

	public ConstFunctor(V value) {
		this.value = value;
	}
}
