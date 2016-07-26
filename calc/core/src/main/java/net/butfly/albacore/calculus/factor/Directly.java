package net.butfly.albacore.calculus.factor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.function.Supplier;

import net.butfly.albacore.calculus.factor.Factor.Type;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Directly {
	StockingDirectly[] value();

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	@Repeatable(Directly.class)
	public @interface StockingDirectly {
		Type type();

		String calc();

		String source();

		Class<? extends Supplier<String>> query();
	}
}
