package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.lambda.Converter;

public interface QueueMap<K, I, O> extends AbstractQueue<I, O> {
	Set<K> keys();

	void initialize(Map<K, ? extends Queue<I, O>> queues);

	long size(K key);

	boolean empty(K key);

	default void close() {}

	O dequeue(K key);

	List<O> dequeue(long batchSize, @SuppressWarnings("unchecked") K... key);

	boolean enqueue(K key, I e);

	long enqueue(Converter<I, K> key, Iterable<I> it);

	long enqueue(Converter<I, K> key, @SuppressWarnings("unchecked") I... e);

	long enqueue(Converter<I, K> key, Iterator<I> iter);

	default Pump<O> pump(QueueMap<K, O, ?> dest, long batchSize, int parallelismPerKey) {
		return pump(dest, batchSize, parallelismPerKey, k -> k);
	}

	@SuppressWarnings("unchecked")
	default <K1> Pump<O> pump(QueueMap<K1, O, ?> dest, long batchSize, int parallelismPerKey, Converter<K, K1> keying) {
		Pump<O> p = new Pump<O>(parallelismPerKey * keys().size(), this, dest);
		for (K k : keys())
			p.submit(() -> dest.enqueue(e -> keying.apply(k), dequeue(batchSize, k)) <= 0, parallelismPerKey);
		return p;
	}
}
