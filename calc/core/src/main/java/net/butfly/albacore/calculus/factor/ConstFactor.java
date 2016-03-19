package net.butfly.albacore.calculus.factor;

import net.butfly.albacore.calculus.factor.Factor.Stocking;
import net.butfly.albacore.calculus.factor.Factor.Type;

@Stocking(type = Type.CONSTAND_TO_CONSOLE)
public class ConstFactor<V> extends Factor<ConstFactor<V>> {
	private static final long serialVersionUID = 9100426079561362807L;
	public V value;

	public ConstFactor(V value) {
		this.value = value;
	}

	@Override
	public String toString() {
		return value == null ? null : value.toString();
	}
}
