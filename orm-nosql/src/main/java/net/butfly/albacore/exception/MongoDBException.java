package net.butfly.albacore.exception;


public class MongoDBException extends DatabaseException {
	private static final long serialVersionUID = 3697666641023435666L;

	public MongoDBException(String code) {
		super(code);
	}

	public MongoDBException(String code, String message, Throwable cause) {
		super(code, message, cause);
	}

	public MongoDBException(String code, String message) {
		super(code, message);
	}

	public MongoDBException(String code, Throwable cause) {
		super(code, cause);
	}
}
