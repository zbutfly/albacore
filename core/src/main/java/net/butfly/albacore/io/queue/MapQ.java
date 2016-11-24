package net.butfly.albacore.io.queue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;
import net.butfly.albacore.utils.async.Concurrents;

public interface MapQ<K, I, O> extends Q<I, O> {
	Set<K> keys();

	default long size(K key) {
		return containKey(key) ? 0 : q(key).size();
	}

	default boolean empty(K key) {
		return containKey(key) ? true : q(key).empty();
	}

	default void close() {
		for (K k : keys())
			if (containKey(k)) q(k).close();

	}

	default List<O> dequeue(long batchSize, K key) {
		return containKey(key) ? new ArrayList<>() : q(key).dequeue(batchSize);
	}

	default boolean containKey(K key) {
		return false;
	}

	default Q<I, O> q(K key) {
		return null;
	}

	/**
	 * basic, none blocking writing.
	 * 
	 * @param d
	 * @return
	 */
	long enqueue(K key, Iterator<I> e);

	long enqueue(Converter<I, K> keying, Iterator<I> iter);

	default long enqueue(Converter<I, K> keying, Iterable<I> it) {
		return enqueue(keying, it.iterator());
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
	default O dequeue() {
		for (K k : Collections.disorderize(keys())) {
			O o = q(k).dequeue();
			if (null != o) return o;
		}
		return null;
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
				List<O> e = dequeue(batchSize, k);
				if (null != e && e.size() > 0) batch.addAll(e);
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
