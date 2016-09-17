package net.butfly.albacore.serializer.modifier;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD, ElementType.TYPE })
public @interface HbaseColumnFamily {
	public static final String DEFAULT_COLUMN_FAMILY = "cf1";

	String value() default DEFAULT_COLUMN_FAMILY;
}
