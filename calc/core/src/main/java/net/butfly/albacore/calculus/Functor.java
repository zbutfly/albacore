package net.butfly.albacore.calculus;

import java.io.Serializable;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import net.butfly.albacore.calculus.marshall.DefaultMarshaller;
import net.butfly.albacore.calculus.marshall.Marshaller;

public interface Functor<F extends Functor<F>> extends Serializable {
	static final String NOT_DEFINED = "";

	public enum Type {
		CONSTAND_TO_CONSOLE, HBASE, MONGODB, KAFKA
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	public @interface Stocking {

		Type type();

		String source() default NOT_DEFINED;

		String table() default NOT_DEFINED;

		String filter() default NOT_DEFINED;

		Class<? extends Marshaller<?, ?>> marshaller() default DefaultMarshaller.class;
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	public @interface Streaming {
		Type type();

		String source();

		String[] topics() default {};

		Class<? extends Marshaller<?, ?>> marshaller() default DefaultMarshaller.class;
	}

	public class VoidFunctor implements Functor<VoidFunctor> {
		private static final long serialVersionUID = -5722216150920437482L;
	}
}
