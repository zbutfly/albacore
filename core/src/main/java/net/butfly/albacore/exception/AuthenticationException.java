package net.butfly.albacore.exception;

import net.butfly.albacore.utils.Exceptions;

public class AuthenticationException extends BusinessException {
	private static final long serialVersionUID = -4154151928102997805L;

	public AuthenticationException() {
		super(Exceptions.Code.AUTH_CODE, "Authentication Failure.");
	}

	public AuthenticationException(String message) {
		super(Exceptions.Code.AUTH_CODE, message);
	}

	public AuthenticationException(Throwable cause) {
		super(Exceptions.Code.AUTH_CODE, "Authentication Failure.", cause);
	}
}
