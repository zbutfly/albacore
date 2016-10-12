package net.butfly.albacore.io;

public interface IterableQueue<E> {
	boolean hasNext();

	default boolean empty() {
		return hasNext();
	}
}
