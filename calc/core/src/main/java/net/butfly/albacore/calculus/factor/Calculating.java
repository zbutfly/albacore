package net.butfly.albacore.calculus.factor;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.spark.storage.StorageLevel;

import net.butfly.albacore.calculus.streaming.RDDDStream.Mechanism;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(Calculating.Calculatings.class)
public @interface Calculating {
	Class<? extends Factor<?>> factor();

	String key() default "";

	@Deprecated
	long batching() default 0L;

	Mechanism stockOnStreaming() default Mechanism.CONST;

	@Retention(RetentionPolicy.RUNTIME)
	@Target(ElementType.TYPE)
	public @interface Calculatings {
		Calculating[] value();
	}

	float expanding()

	default 1;

	Persisting persisting() default Persisting.NONE;

	public enum Persisting {
		NONE(StorageLevel.NONE()), //
		DISK_ONLY(StorageLevel.DISK_ONLY()), //
		DISK_ONLY_2(StorageLevel.DISK_ONLY_2()), //
		MEMORY_ONLY(StorageLevel.MEMORY_ONLY()), //
		MEMORY_ONLY_2(StorageLevel.MEMORY_ONLY_2()), //
		MEMORY_ONLY_SER(StorageLevel.MEMORY_ONLY_SER()), //
		MEMORY_ONLY_SER_2(StorageLevel.MEMORY_ONLY_SER_2()), //
		MEMORY_AND_DISK(StorageLevel.MEMORY_AND_DISK()), //
		MEMORY_AND_DISK_2(StorageLevel.MEMORY_AND_DISK_2()), //
		MEMORY_AND_DISK_SER(StorageLevel.MEMORY_AND_DISK_SER()), //
		MEMORY_AND_DISK_SER_2(StorageLevel.MEMORY_AND_DISK_SER_2()), //
		OFF_HEAP(StorageLevel.OFF_HEAP());

		private StorageLevel level;

		public StorageLevel level() {
			return level;
		}

		private Persisting(StorageLevel level) {
			this.level = level;
		}
	}
}
