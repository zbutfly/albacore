package net.butfly.albacore.calculus;

public @interface Calculus {
	Class<? extends Functor<?>>[] value();
}
