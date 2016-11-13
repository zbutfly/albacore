package net.butfly.albacore.serder.support;

import net.butfly.albacore.serder.Serder;

public interface ClassInfoSerder<P, D> extends Serder<P, D> {
	<TT extends P> TT der(D from);

	@Override
	default <TT extends P> TT der(D from, Class<TT> to) {
		return der(from);
	}

	default <RESULT> ClassInfoSerder<P, RESULT> then(ClassInfoSerder<D, RESULT> next, Class<D> dataClass) {
		return new ClassInfoSerder<P, RESULT>() {
			private static final long serialVersionUID = -3404957161619159155L;

			@Override
			public RESULT ser(P from) {
				return next.ser(ClassInfoSerder.this.ser(from));
			}

			@Override
			public <T extends P> T der(RESULT from) {
				return ClassInfoSerder.this.der(next.der(from));
			}
		};
	}
}
