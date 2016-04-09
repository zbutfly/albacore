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
		Type type();

		String source() default NOT_DEFINED;

		String[] table() default {};

		String filter() default NOT_DEFINED;
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	public @interface Streaming {
		Type type();

		String source();

		String[] table() default {};
	}

	public final static class VoidFactor extends Factor<VoidFactor> {
		private static final long serialVersionUID = -5722216150920437482L;

		private VoidFactor() {}
	}

	@Stocking(type = Type.CONSTAND_TO_CONSOLE)
	public final static class Const<V> extends Factor<Const<V>> {
		private static final long serialVersionUID = 9100426079561362807L;
		public V value;

		public Const(V value) {
			this.value = value;
		}

		@Override
		public String toString() {
			return value == null ? "null" : value.toString();
		}
	}
}
