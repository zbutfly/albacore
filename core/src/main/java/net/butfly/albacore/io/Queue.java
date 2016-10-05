package net.butfly.albacore.io;

import java.io.Closeable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import net.butfly.albacore.exception.NotImplementedException;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Reflections;

/**
 * Rich feature queue for big data processing, supporting:
 * <ul>
 * <li>Blocking based on capacity</li>
 * <li>Batching</li>
 * <ul>
 * <li>Batching in restrict synchronous or not</li>
 * </ul>
 * <li>Storage/pooling policies</li>
 * <ul>
 * <li>Instant</li>
 * <li>Memory (heap)</li>
 * <li>Local disk (off heap based on memory mapping), like
 * {@link MapDB}/{@link BigQueue} and so on</li>
 * <li>Remote, like Kafka/MQ and so on (Not now)</li>
 * </ul>
 * <li>Continuous or not</li>
 * <li>Connect to another ("then op", into engine named "Pump")</li>
 * <ul>
 * <li>Fan out to others ("thens op", to {@link KeyQueue})</li>
 * <li>Merge into {@link KeyQueue}</li>
 * </ul>
 * <li>Statistic</li>
 * </ul>
 * 
 * @author butfly
 *
 */
public interface Queue<E> extends Closeable, Serializable, Runnable {

	/* from Queue */

	boolean enqueue(E e);

	E dequeue();

	/* for rich features */

	String name();

	long size();

	long capacity();

	default boolean empty() {
		return size() == 0;
	};

	default boolean full() {
		return size() >= capacity();
	};

	default long enqueue(Iterator<E> iter) {
		long c = 0;
		while (iter.hasNext()) {
			E e = iter.next();
			if (null != e && enqueue(e)) c++;
		}
		return c;
	}

	default long enqueue(Iterable<E> it) {
		return enqueue(it.iterator());
	}

	@SuppressWarnings("unchecked")
	default long enqueue(E... e) {
		long c = 0;
		for (int i = 0; i < e.length; i++)
			if (e[i] != null && enqueue(e[i])) c++;
		return c;
	}

	default List<E> dequeue(long batchSize) {
		List<E> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			E e = dequeue();
			if (null != e) {
				batch.add(e);
				stats(e, false);
			}
		} while (batch.size() < batchSize && prev != batch.size());
		if (empty()) gc();
		return batch;
	}

	void stats(E e, boolean trueInOrFalseOut);

	boolean isReadOrderly();

	boolean isWriteOrderly();

	void setReadOrderly(boolean orderly);

	void setWriteOrderly(boolean orderly);

	/* from interfaces */

	@Override
	void close();

	@Override
	void run();

	boolean running();

	default void gc() {}

	/* lambda */

	default <V> AbstractQueue<V> serder(Converter<E, V> conv, Converter<V, E> unconv) {
		Reflections.noneNull("", conv, unconv);
		return new AbstractQueue<V>(Queue.this.name() + "-wrapper", Queue.this.capacity()) {
			private static final long serialVersionUID = -4723864334868408130L;

			@Override
			public boolean enqueueRaw(V e) {
				if (null == e) return false;
				return Queue.this.enqueue(unconv.apply(e));
			}

			@Override
			public V dequeueRaw() {
				E e = Queue.this.dequeue();
				return null == e ? null : conv.apply(e);
			}

			@Override
			public long size() {
				return Queue.this.size();
			}

			@Override
			public void close() {
				Queue.this.close();
			}
		};
	}

	default AbstractQueue<E> java(java.util.Queue<E> javaQueue) {
		throw new NotImplementedException();
	}

	default <K, Q extends AbstractQueue<E>> MapQueue<K, E> fanout(Converter<E, K> keying, Converter<K, Q> constructor) {
		Reflections.noneNull("", keying, constructor);
		return new MapQueueImpl<K, E, Q>(name() + "-fanout", keying, constructor, capacity());
	}

	default <K, V, Q extends AbstractQueue<V>> MapQueue<K, V> fanout(Converter<V, K> keying, Converter<K, Q> constructor,
			Converter<E, V> conv, Converter<V, E> unconv) {
		Reflections.noneNull("", keying, constructor, conv, unconv);

		return new MapQueueImpl<K, V, Q>(Queue.this.name() + "-fanout", keying::apply, k -> {
			@SuppressWarnings("unchecked")
			Queue<V> r = ((MapQueueImpl<K, E, ?>) this).constructor.apply(k).serder(conv, unconv);
			return (Q) r;
		}, Queue.this.capacity());
	}
}
