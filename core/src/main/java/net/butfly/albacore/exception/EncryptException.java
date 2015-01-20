package net.butfly.albacore.exception;

public class EncryptException extends SystemException {
	private static final long serialVersionUID = -6625265141542101082L;

	public EncryptException(String message, Throwable cause) {
		super(Exceptions.ENCRYPT_CODE, message, cause);
	}

	public EncryptException(String message) {
		super(Exceptions.ENCRYPT_CODE, message);
	}

	public EncryptException(Throwable cause) {
		super(Exceptions.ENCRYPT_CODE, cause);
	}
}
