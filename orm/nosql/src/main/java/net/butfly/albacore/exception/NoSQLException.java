package net.butfly.albacore.exception;


public class NoSQLException extends DatabaseException {
	private static final long serialVersionUID = 3697666641023435666L;

	public NoSQLException(String code) {
		super(code);
	}

	public NoSQLException(String code, String message, Throwable cause) {
		super(code, message, cause);
	}

	public NoSQLException(String code, String message) {
		super(code, message);
	}

	public NoSQLException(String code, Throwable cause) {
		super(code, cause);
	}
}
