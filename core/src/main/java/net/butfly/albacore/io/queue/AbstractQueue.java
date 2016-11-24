package net.butfly.albacore.io.queue;

import java.io.Closeable;
import java.io.Serializable;

interface AbstractQueue<I, O> extends Closeable, Serializable {
	static final long INFINITE_SIZE = -1;
	static final long FULL_WAIT_MS = 500;
	static final long EMPTY_WAIT_MS = 500;

	String name();
}