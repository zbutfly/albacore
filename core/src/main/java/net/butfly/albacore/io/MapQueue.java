package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.List;

import net.butfly.albacore.lambda.Converter;

public interface MapQueue<K, IN, OUT, DATA> extends Queue<IN, OUT, DATA> {
	boolean empty(K key);

	OUT dequeue(K key);

	List<OUT> dequeue(long batchSize, @SuppressWarnings("unchecked") K... key);

	boolean enqueue(K key, IN e);

	long enqueue(Converter<IN, K> key, @SuppressWarnings("unchecked") IN... e);

	default long enqueue(Converter<IN, K> key, Iterable<IN> it) {
		return enqueue(key, it.iterator());
	};

	long enqueue(Converter<IN, K> key, Iterator<IN> iter);

	long size(K key);
}
