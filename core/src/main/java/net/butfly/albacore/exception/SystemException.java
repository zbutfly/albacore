package net.butfly.albacore.exception;

import java.io.PrintStream;

import net.butfly.albacore.utils.Exceptions;

public class SystemException extends RuntimeException {
	private static final long serialVersionUID = -7617925338905953846L;
	protected String code;

	public String getCode() {
		return code;
	}

	public SystemException(String code) {
		super();
		this.code = code;
	}

	public SystemException(String code, String message) {
		super(message);
		this.code = code;
	}

	public SystemException(String code, Throwable cause) {
		super(cause);
		this.code = code;
	}

	public SystemException(String code, String message, Throwable cause) {
		super(message, cause);
		this.code = code;
	}

	public void printStackTrace(PrintStream s) {
		Exceptions.printStackTrace(this, this.code, s);
	}
}
