package net.butfly.albacore.calculus;

public @interface Calculus {
	public enum Mode {
		STOCKING, STREAMING
	}

	Class<? extends Functor<?>>[] stocking();

	Class<? extends Functor<?>>[] streaming() default {};

	Class<? extends Functor<?>> saving();
}
