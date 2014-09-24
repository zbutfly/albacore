package net.butfly.albacore.exception;

public class EncryptException extends SystemException {

	private static final long serialVersionUID = -6625265141542101082L;
	private final static String CODE = "SYS_087";

	public EncryptException() {
		super(CODE);
	}

	public EncryptException(String message) {
		super(CODE, message);
	}

	public EncryptException(Throwable cause) {
		super(CODE, cause);
	}

	public EncryptException(String message, Throwable cause) {
		super(CODE, message, cause);
	}
}
