package net.butfly.albacore.calculus.factor;

import net.butfly.albacore.calculus.factor.Factor.Stocking;
import net.butfly.albacore.calculus.factor.Factor.Type;

@Stocking(type = Type.CONSTAND_TO_CONSOLE)
public class IntegerFactor extends Factor<IntegerFactor> {
	private static final long serialVersionUID = 9100426079561362807L;
	public int value;

	public IntegerFactor(String str) {
		this(Integer.parseInt(str));
	}

	public IntegerFactor(int value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return Integer.toString(value);
	}
}
