package net.butfly.albacore.calculus.functor;

import net.butfly.albacore.calculus.Functor;
import net.butfly.albacore.calculus.Functor.Stocking;
import net.butfly.albacore.calculus.Functor.Type;

@Stocking(type = Type.CONSTAND_TO_CONSOLE)
public class IntegerFunctor extends ConstFunctor<Integer> implements Functor<ConstFunctor<Integer>> {
	private static final long serialVersionUID = 9100426079561362807L;

	public IntegerFunctor(String str) {
		super(str);
	}

	public IntegerFunctor(Integer value) {
		super(value);
	}

	@Override
	public String toString() {
		return Integer.toString(value);
	}
}
