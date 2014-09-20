package net.butfly.albacore.helper.swift.exception;

public class AuthenticationFailureException extends SwiftException {
	private static final long serialVersionUID = 7915008337658043485L;

	public AuthenticationFailureException() {
		super();
	}

	public AuthenticationFailureException(String message) {
		super(message);
	}

	public AuthenticationFailureException(Throwable cause) {
		super(cause);
	}
}
