package net.butfly.albacore.calculus.factor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.function.Supplier;

import net.butfly.albacore.calculus.Calculator.Mode;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(Factoring.Factorings.class)
public @interface Factoring {

	Mode mode() default Mode.STOCKING;

	String key() default "";

	String ds();

	String[] table() default {};

	String[] query() default {};

	Class<? extends Supplier<String>>[] querying() default {};

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface Factorings {
		Factoring[] value();
	}

}
