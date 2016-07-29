package net.butfly.albacore.calculus.factor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(Calculating.Calculatings.class)
public @interface Calculating {
	Class<? extends Factor<?>> factor();

	String key();

	@Deprecated
	long batching()

	default 0L;

	Mechanism stockOnStreaming()

	default Mechanism.CONST;

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface Calculatings {
		Calculating[] value();
	}

	float expanding() default 1;

	String persisting() default PERSIST_NONE;

	public static final String PERSIST_NONE = "NONE";
	public static final String PERSIST_DISK_ONLY = "DISK_ONLY";
	public static final String PERSIST_DISK_ONLY_2 = "DISK_ONLY_2";
	public static final String PERSIST_MEMORY_ONLY = "MEMORY_ONLY";
	public static final String PERSIST_MEMORY_ONLY_2 = "MEMORY_ONLY_2";
	public static final String PERSIST_MEMORY_ONLY_SER = "MEMORY_ONLY_SER";
	public static final String PERSIST_MEMORY_ONLY_SER_2 = "MEMORY_ONLY_SER_2";
	public static final String PERSIST_MEMORY_AND_DISK = "MEMORY_AND_DISK";
	public static final String PERSIST_MEMORY_AND_DISK_2 = "MEMORY_AND_DISK_2";
	public static final String PERSIST_MEMORY_AND_DISK_SER = "MEMORY_AND_DISK_SER";
	public static final String PERSIST_MEMORY_AND_DISK_SER_2 = "MEMORY_AND_DISK_SER_2";
	public static final String PERSIST_OFF_HEAP = "OFF_HEAP";
}
