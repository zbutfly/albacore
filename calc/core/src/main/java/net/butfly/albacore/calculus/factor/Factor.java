package net.butfly.albacore.calculus.factor;

import java.io.Serializable;

public interface Factor<F extends Factor<F>> extends Serializable {
	public static final String NOT_DEFINED = "";

	public final static class VoidFactor implements Factor<VoidFactor> {
		private static final long serialVersionUID = -5722216150920437482L;

		private VoidFactor() {}
	}

	@Factoring(ds = "CONSOLE")
	public final static class Const<V> implements Factor<Const<V>> {
		private static final long serialVersionUID = 9100426079561362807L;
		public V value;

		public Const(V value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return value == null ? "null" : value.toString();
		}
	}
}
