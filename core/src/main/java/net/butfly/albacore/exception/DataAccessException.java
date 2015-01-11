package net.butfly.albacore.exception;

public class DataAccessException extends SystemException {
	private static final long serialVersionUID = -7617925338905953846L;

	public DataAccessException(String message, Throwable cause) {
		super(Exceptions.DATA_ACCESS_CODE, message, cause);
	}

	public DataAccessException(String message) {
		super(Exceptions.DATA_ACCESS_CODE, message);
	}
}
