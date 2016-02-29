package net.butfly.albacore.calculus;

import java.io.Serializable;

public interface Functor<F extends Functor<F>> extends Serializable {
	public enum Type {
		HBASE, MONGODB, KAFKA
	}

	public @interface Stocking {
		Type type();

		String source();

		String table();

		String filter() default "";
	}

	public @interface Streaming {
		Type type();

		String source();

		String[] topics();
	}
}
