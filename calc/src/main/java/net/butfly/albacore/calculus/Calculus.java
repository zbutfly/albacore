package net.butfly.albacore.calculus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface Calculus {
	public enum Mode {
		STOCKING, STREAMING
	}

	Class<? extends Functor<?>>[] stocking();

	Class<? extends Functor<?>>[] streaming() default {};

	Class<? extends Functor<?>> saving();
}
