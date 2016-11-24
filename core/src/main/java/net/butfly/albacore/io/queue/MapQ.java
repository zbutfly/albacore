package net.butfly.albacore.io.queue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;

public interface MapQ<K, I, O> extends Q<I, O> {
	default Q<I, O> q(K key) {
		return null;
	}

	default Set<K> keys() {
		return new HashSet<>();
	}

	default long size(K key) {
		Q<I, O> q = q(key);
		return null == q ? 0 : q.size();
	}

	default boolean empty(K key) {
		Q<I, O> q = q(key);
		return null == q ? true : q.empty();
	}

	default void close() {
		for (K k : keys())
			q(k).close();
	}

	/**
	 * basic, none blocking reading.
	 * 
	 * @param key
	 * @return
	 */
	default O dequeue0(K key) {
		Q<I, O> q = q(key);
		return null == q ? null : q(key).dequeue0();
	}

	default List<O> dequeue(long batchSize, @SuppressWarnings("unchecked") K... key) {
		List<O> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			for (K k : key) {
				O e = dequeue0(k);
				if (null != e) {
					batch.add(e);
					if (batch.size() >= batchSize) return batch;
				}
			}
			if (batch.size() == 0) Concurrents.waitSleep(EMPTY_WAIT_MS);
		} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		return batch;
	}

	/**
	 * basic, none blocking writing.
	 * 
	 * @param key
	 * @return
	 */
	boolean enqueue0(K key, I item);

	default long enqueue(Converter<I, K> keying, List<I> items) {
		while (full())
			Concurrents.waitSleep(FULL_WAIT_MS);
		return MapQImpl.enqueueAll(i -> q(keying.apply(i)), items);
	}

	default long enqueue(Converter<I, K> keying, @SuppressWarnings("unchecked") I... e) {
		return enqueue(keying, Arrays.asList(e));
	}

	/** from Q */
	@Override
	default long size() {
		long s = 0;
		for (K k : keys())
			s += size(k);
		return s;
	}

	@Override
	default List<O> dequeue(long batchSize) {
		List<O> batch = new ArrayList<>();
		List<K> ks = Collections.disorderize(keys());
		int prev;
		boolean continuing = false;
		do {
			prev = batch.size();
			for (K k : ks) {
				O e = ((MapQImpl<K, I, O>) this).dequeue0(k);
				if (null != e) batch.add(e);
			}
			if (batch.size() == 0) Concurrents.waitSleep(EMPTY_WAIT_MS);
			continuing = batch.size() < batchSize && (prev != batch.size() || batch.size() == 0);
			if (continuing) ks = Collections.disorderize(ks);
		} while (continuing);
		return batch;
	}

	/**************/
	default void setReadOrderly(boolean orderly) {
		throw new UnsupportedOperationException();
	}

	default void setWriteOrderly(boolean orderly) {
		throw new UnsupportedOperationException();
	}

	default boolean isReadOrderly() {
		throw new UnsupportedOperationException();
	}

	default boolean isWriteOrderly() {
		throw new UnsupportedOperationException();
	}

}
