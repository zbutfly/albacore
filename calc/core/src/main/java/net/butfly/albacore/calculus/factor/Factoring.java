package net.butfly.albacore.calculus.factor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(Factoring.Factorings.class)
public @interface Factoring {
	Class<? extends Factor<?>> factor();

	String key();

	@Deprecated
	long batching()

	default 0L;

	Mechanism stockOnStreaming()

	default Mechanism.CONST;

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface Factorings {
		Factoring[] value();
	}

	int expanding() default 1;

	String persisting() default "";
}
