package net.butfly.albacore.helper.swift.exception;

import net.butfly.albacore.exception.BusinessException;

public class SwiftException extends BusinessException {
	private static final long serialVersionUID = 7587500560631008370L;
	public static final String SWIFT_ERROR_CODE = "SWIFT_000";

	public SwiftException() {
		super(SWIFT_ERROR_CODE);
	}

	public SwiftException(String message) {
		super(SWIFT_ERROR_CODE, message);
	}

	public SwiftException(Throwable cause) {
		super(SWIFT_ERROR_CODE, cause);
	}

	public SwiftException(String message, Throwable cause) {
		super(SWIFT_ERROR_CODE, message, cause);
	}
}
