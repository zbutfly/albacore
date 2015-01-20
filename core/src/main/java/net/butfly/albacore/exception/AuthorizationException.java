package net.butfly.albacore.exception;

public class AuthorizationException extends BusinessException {
	private static final long serialVersionUID = -4154151928102997805L;

	public AuthorizationException() {
		super(Exceptions.AUTH_CODE);
	}

	public AuthorizationException(String message) {
		super(Exceptions.AUTH_CODE, message);
	}

	public AuthorizationException(Throwable cause) {
		super(Exceptions.AUTH_CODE, cause);
	}
}
