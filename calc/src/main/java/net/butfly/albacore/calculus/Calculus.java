package net.butfly.albacore.calculus;

public @interface Calculus {
	enum Mode {
		STOCKING, STREAMING
	}

	Class<? extends Functor<?>>[] masters();

	Class<? extends Functor<?>>[] vices() default {};

	Class<? extends Functor<?>> destination();
}
