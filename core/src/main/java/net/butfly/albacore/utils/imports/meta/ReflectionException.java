package net.butfly.albacore.utils.imports.meta;

public class ReflectionException extends RuntimeException {
	private static final long serialVersionUID = 7800730905768238715L;

	public ReflectionException() {
		super();
	}

	public ReflectionException(String message) {
		super(message);
	}

	public ReflectionException(String message, Throwable cause) {
		super(message, cause);
	}

	public ReflectionException(Throwable cause) {
		super(cause);
	}
}
