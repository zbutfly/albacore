package net.butfly.albacore.calculus.factor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(Factoring.Factorings.class)
public @interface Factoring {
	Class<? extends Factor<?>> factor();

	String key();

	long batching() default 0L;

	OnStreaming stockOnStreaming() default OnStreaming.ONCE;

	String foreignKey() default Factor.NOT_DEFINED;

	Join join() default Join.RIGHT;

	String primaryFactor() default Factor.NOT_DEFINED;

	String primaryKey() default Factor.NOT_DEFINED;

	public enum Join {
		INNER, LEFT, RIGHT
	}

	public enum OnStreaming {
		ONCE, EACH, CACHE
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface Factorings {
		Factoring[] value();
	}
}
