package net.butfly.albacore.serder;

import net.butfly.albacore.lambda.Converter;

public interface ClassInfoSerder<P, D> extends Serder<P, D> {
	<T extends P> T der(D from);

	@Override
	default <T extends P> T der(D from, Class<T> to) {
		return der(from);
	}

	default <T extends P> Converter<D, T> unconverter() {
		return this::der;
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
