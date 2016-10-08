package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.async.Concurrents;

public interface MapQueue<K, I, O> extends Queue<I, O> {
	boolean empty(K key);

	O dequeue(K key);

	List<O> dequeue(long batchSize, @SuppressWarnings("unchecked") K... key);

	boolean enqueue(K key, I e);

	long enqueue(Converter<I, K> key, Iterable<I> it);

	long enqueue(Converter<I, K> key, @SuppressWarnings("unchecked") I... e);

	long enqueue(Converter<I, K> key, Iterator<I> iter);

	long size(K key);

	default void pump(MapQueueImpl<K, O, ?, ?> dest, long batchSize, int parallelism) {
		ExecutorService ex = Concurrents.executor(parallelism, this.name(), dest.name());
		for (int i = 0; i < parallelism; i++)
			ex.submit(new Thread(new Runnable() {
				@Override
				public void run() {
					dest.enqueue(dest::keying, dequeue(batchSize));
				}
			}), this.name() + "-" + dest.name() + "-" + i);
	}

	MapQueue<K, I, O> initialize(Map<K, ? extends Queue<I, O>> queues);
}
