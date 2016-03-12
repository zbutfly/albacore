package net.butfly.albacore.calculus.functor;

import net.butfly.albacore.calculus.Functor;
import net.butfly.albacore.calculus.Functor.Stocking;
import net.butfly.albacore.calculus.Functor.Type;

@Stocking(type = Type.CONSTAND_TO_CONSOLE)
public class IntegerFunctor implements Functor<IntegerFunctor> {
	private static final long serialVersionUID = 9100426079561362807L;
	public int value;

	public IntegerFunctor(String str) {
		this(Integer.parseInt(str));
	}

	public IntegerFunctor(int value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return Integer.toString(value);
	}
}
