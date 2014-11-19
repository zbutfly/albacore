package net.butfly.albacore.exception;

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
}
