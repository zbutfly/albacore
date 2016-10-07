package net.butfly.albacore.io;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

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
 * <li>Local disk (off heap based on memory mapping), like {@link MapDB}/
 * {@link BigQueue} and so on</li>
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
public interface Queue<E> extends Closeable, Serializable {

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

	List<E> dequeue(long batchSize);

	boolean isReadOrderly();

	boolean isWriteOrderly();

	void setReadOrderly(boolean orderly);

	void setWriteOrderly(boolean orderly);

	/* from interfaces */

	@Override
	default void close() {}

	default void gc() {}
}
