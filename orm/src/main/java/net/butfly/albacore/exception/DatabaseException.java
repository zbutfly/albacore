package net.butfly.albacore.exception;

public class DatabaseException extends SystemException {
	private static final long serialVersionUID = 3697666641023435666L;

	public DatabaseException(String code) {
		super(code);
	}

	public DatabaseException(String code, String message, Throwable cause) {
		super(code, message, cause);
	}

	public DatabaseException(String code, String message) {
		super(code, message);
	}

	public DatabaseException(String code, Throwable cause) {
		super(code, cause);
	}
}
