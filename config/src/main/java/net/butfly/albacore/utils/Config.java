package net.butfly.albacore.utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.FIELD })
public @interface Config {
	static final String NOT_DEFINE = "";

	String value() default NOT_DEFINE;

	String prefix() default NOT_DEFINE;
}
