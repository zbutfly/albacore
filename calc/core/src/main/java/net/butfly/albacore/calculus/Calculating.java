package net.butfly.albacore.calculus;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import net.butfly.albacore.calculus.Functor.VoidFunctor;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface Calculating {
	public enum Mode {
		STOCKING, STREAMING
	}

	Class<? extends Functor<?>>[] value() default {};

	Class<? extends Functor<?>> saving() default VoidFunctor.class;
}
