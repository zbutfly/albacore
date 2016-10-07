package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.async.Concurrents;

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

	default void pump(MapQueue<K, OUT, ?, ?> dest, long batchSize, int parallelism) {
		ExecutorService ex = Concurrents.executor(parallelism, this.name(), dest.name());
		for (int i = 0; i < parallelism; i++)
			ex.submit(new Thread(new Runnable() {
				@Override
				public void run() {

				}
			}), this.name() + "-" + dest.name() + "-" + i);
	}
}
