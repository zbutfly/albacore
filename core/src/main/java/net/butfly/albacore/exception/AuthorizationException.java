package net.butfly.albacore.exception;

import net.butfly.albacore.utils.Exceptions;

public class AuthorizationException extends BusinessException {
	private static final long serialVersionUID = -4154151928102997805L;

	public AuthorizationException() {
		super(Exceptions.Code.AUTH_CODE);
	}

	public AuthorizationException(String message) {
		super(Exceptions.Code.AUTH_CODE, message);
	}

	public AuthorizationException(Throwable cause) {
		super(Exceptions.Code.AUTH_CODE, cause);
	}
}
