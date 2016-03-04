package net.butfly.albacore.calculus;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public interface Functor<F extends Functor<F>> extends Serializable {
	static final String NOT_DEFINED = "";

	public enum Type {
		CONST, CONSOLE, HBASE, MONGODB, KAFKA
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	public @interface Stocking {
		Type type();

		String source();

		String table() default NOT_DEFINED;

		String filter() default NOT_DEFINED;
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	public @interface Streaming {
		Type type();

		String source();

		String[] topics() default {};
	}
}
