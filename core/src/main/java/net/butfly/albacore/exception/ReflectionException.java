package net.butfly.albacore.exception;

public class ReflectionException extends SystemException {
	private static final long serialVersionUID = -7617925338905953846L;

	public ReflectionException(String message, Throwable cause) {
		super(Exceptions.REFLEC_CODE, message, cause);
	}

	public ReflectionException(String message) {
		super(Exceptions.REFLEC_CODE, message);
	}
}
