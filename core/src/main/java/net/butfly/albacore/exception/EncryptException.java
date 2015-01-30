package net.butfly.albacore.exception;

import net.butfly.albacore.utils.Exceptions;

public class EncryptException extends SystemException {
	private static final long serialVersionUID = -6625265141542101082L;

	public EncryptException(String message, Throwable cause) {
		super(Exceptions.Code.ENCRYPT_CODE, message, cause);
	}

	public EncryptException(String message) {
		super(Exceptions.Code.ENCRYPT_CODE, message);
	}

	public EncryptException(Throwable cause) {
		super(Exceptions.Code.ENCRYPT_CODE, cause);
	}
}
