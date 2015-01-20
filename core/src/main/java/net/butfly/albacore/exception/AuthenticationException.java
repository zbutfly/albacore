package net.butfly.albacore.exception;

public class AuthenticationException extends BusinessException {
	private static final long serialVersionUID = -4154151928102997805L;

	public AuthenticationException() {
		super(Exceptions.AUTH_CODE, "Authentication Failure.");
	}

	public AuthenticationException(String message) {
		super(Exceptions.AUTH_CODE, message);
	}

	public AuthenticationException(Throwable cause) {
		super(Exceptions.AUTH_CODE, "Authentication Failure.", cause);
	}
}
