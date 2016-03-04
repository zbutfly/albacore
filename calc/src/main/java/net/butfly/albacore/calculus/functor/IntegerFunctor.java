package net.butfly.albacore.calculus.functor;

import net.butfly.albacore.calculus.Functor;

public abstract class IntegerFunctor extends ConstFunctor<Integer> implements Functor<ConstFunctor<Integer>> {
	private static final long serialVersionUID = 9100426079561362807L;

	public IntegerFunctor(String str) {
		this.value = Integer.parseInt(str);
	}
}
