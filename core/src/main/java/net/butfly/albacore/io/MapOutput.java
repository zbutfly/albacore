package net.butfly.albacore.io;

import java.util.List;
import java.util.Set;

import net.butfly.albacore.io.queue.MapQ;

public interface MapOutput<K, I> extends MapQ<K, I, Void>, Output<I> {
	@Override
	default boolean full() {
		return false;
	}

	@Override
	default List<Void> dequeue(long batchSize) {
		return null;
	}

	@Override
	default Set<K> keys() {
		return null;
	}

	@Override
	default long size(K key) {
		return 0;
	}

	@Override
	default boolean empty(K key) {
		return false;
	}

	@Override
	default List<Void> dequeue(long batchSize, K key) {
		return null;
	}

	@Override
	default void close() {
		MapQ.super.close();
	}

	@Override
	default long size() {
		return MapQ.super.size();
	}
}
