package net.butfly.albacore.exception;

public class DataAccessException extends SystemException {
	private static final long serialVersionUID = -7617925338905953846L;

	public DataAccessException(String code) {
		super(code);
	}

	public DataAccessException(String code, String message) {
		super(code, message);
	}

	public DataAccessException(String code, String message, Throwable cause) {
		super(code, message, cause);
	}

	public DataAccessException(String code, Throwable cause) {
		super(code, cause);
	}
}
