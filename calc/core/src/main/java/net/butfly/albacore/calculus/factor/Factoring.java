package net.butfly.albacore.calculus.factor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import net.butfly.albacore.calculus.Mode;
import net.butfly.albacore.calculus.factor.Factor.Type;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(Factoring.Factorings.class)
public @interface Factoring {
	Mode mode() default Mode.STOCKING;

	Type type() default Type.MONGODB;

	String ds();

	String[] table() default {};

	String[] query() default {};

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface Factorings {
		Factoring[] value();
	}
}
