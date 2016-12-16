package net.butfly.albacore.io;

public interface Openable extends AutoCloseable {
	enum Status {
		CLOSED, OPENING, OPENED, CLOSING
	}

	void open();

	void close();
}
