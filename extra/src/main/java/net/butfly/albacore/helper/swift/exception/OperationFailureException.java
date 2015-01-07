package net.butfly.albacore.helper.swift.exception;

public class OperationFailureException extends SwiftException {
	private static final long serialVersionUID = -2713505229278105748L;

	public OperationFailureException() {
		super();
	}

	public OperationFailureException(String message) {
		super(message);
	}

	public OperationFailureException(Throwable cause) {
		super(cause);
	}

	public OperationFailureException(String message, Throwable cause) {
		super(message, cause);
	}
}
