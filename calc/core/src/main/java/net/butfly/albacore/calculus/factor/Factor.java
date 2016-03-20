package net.butfly.albacore.calculus.factor;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public abstract class Factor<F extends Factor<F>> implements Serializable {
	private static final long serialVersionUID = 5565735433468269463L;
	public static final String NOT_DEFINED = "";

	public enum Type {
		CONSTAND_TO_CONSOLE, HBASE, MONGODB, KAFKA
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	public @interface Stocking {
		public enum OnStreaming {
			NONE, ONCE, EACH, CACHE
		}

		Type type();

		String source() default NOT_DEFINED;

		String table() default NOT_DEFINED;

		String filter() default NOT_DEFINED;

		long batching() default 0L;

		OnStreaming streaming() default OnStreaming.EACH;
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	public @interface Streaming {
		Type type();

		String source();

		String[] topics() default {};
	}
}
