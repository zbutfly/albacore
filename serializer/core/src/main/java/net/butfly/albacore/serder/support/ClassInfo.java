package net.butfly.albacore.serder.support;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ClassInfo {
	enum ClassInfoSupport {
		LOOSE, RESTRICT
	};

	ClassInfoSupport value() default ClassInfoSupport.LOOSE;
}
