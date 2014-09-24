package net.butfly.albacore.helper.swift.exception;

public class UnknownResponseException extends SwiftException {
	private static final long serialVersionUID = 4556216576318406842L;

	public UnknownResponseException() {
		super();
	}

	public UnknownResponseException(String message) {
		super(message);
	}

	public UnknownResponseException(Throwable cause) {
		super(cause);
	}
}
