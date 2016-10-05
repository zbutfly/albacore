package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.List;

import net.butfly.albacore.lambda.Converter;

public interface MapQueue<K, E> extends Queue<E> {
	boolean empty(K key);

	E dequeue(K key);

	List<E> dequeue(long batchSize, @SuppressWarnings("unchecked") K... key);

	boolean enqueue(K key, E e);

	long enqueue(Converter<E, K> key, @SuppressWarnings("unchecked") E... e);

	long enqueue(Converter<E, K> key, Iterable<E> it);

	long enqueue(Converter<E, K> key, Iterator<E> iter);

	long size(K key);
}
