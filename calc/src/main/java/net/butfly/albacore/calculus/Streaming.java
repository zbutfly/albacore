package net.butfly.albacore.calculus;

public @interface Streaming {
	public enum Type {
		KAFKA
	}

	Type value() default Type.KAFKA;

	String[] topics();
}
