package net.butfly.albacore.calculus.marshall;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.FIELD, ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface HbaseColumnFamily {
	public static final String DEFAULT_COLUMN_FAMILY = "cf1";

	String value() default DEFAULT_COLUMN_FAMILY;
}
