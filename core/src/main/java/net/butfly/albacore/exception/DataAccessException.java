package net.butfly.albacore.exception;

import net.butfly.albacore.utils.Exceptions;

public class DataAccessException extends SystemException {
	private static final long serialVersionUID = -7617925338905953846L;

	public DataAccessException(String message, Throwable cause) {
		super(Exceptions.Code.DATA_CODE, message, cause);
	}

	public DataAccessException(String message) {
		super(Exceptions.Code.DATA_CODE, message);
	}
}
