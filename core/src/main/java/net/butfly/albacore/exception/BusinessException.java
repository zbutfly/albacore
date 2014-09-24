package net.butfly.albacore.exception;

public class BusinessException extends Exception {
	private static final long serialVersionUID = -7617925338905953846L;
	private String code;

	public String getCode() {
		return code;
	}

	public BusinessException(String code) {
		super();
		this.code = code;
	}

	public BusinessException(String code, String message) {
		super(message);
		this.code = code;
	}

	public BusinessException(String code, String message, Throwable cause) {
		super(message, cause);
		this.code = code;
	}

	public BusinessException(String code, Throwable cause) {
		super(cause);
		this.code = code;
	}
}
