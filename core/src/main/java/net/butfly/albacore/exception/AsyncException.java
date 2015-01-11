package net.butfly.albacore.exception;

public class AsyncException extends SystemException {
	private static final long serialVersionUID = 5852815901120451509L;

	public AsyncException(String message, Throwable cause) {
		super(Exceptions.ASYNC_CODE, cause);
	}

	public AsyncException(String message) {
		super(Exceptions.ASYNC_CODE);
	}
}
