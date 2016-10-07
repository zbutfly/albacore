package net.butfly.albacore.io;

import java.io.Closeable;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

import net.butfly.albacore.utils.async.Concurrents;
import net.butfly.albacore.utils.logger.Logger;

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
public interface Queue<IN, OUT, DATA> extends Closeable, Serializable {
	static final Logger logger = Logger.getLogger(AbstractQueue.class);
	static final long INFINITE_SIZE = -1;
	static final long FULL_WAIT_MS = 500;
	static final long EMPTY_WAIT_MS = 500;

	/* from Queue */

	boolean enqueue(IN e);

	OUT dequeue();

	/* for rich features */

	String name();

	long size();

	long capacity();

	default boolean empty() {
		return size() == 0;
	};

	default boolean full() {
		long s = size();
		return s > 0 && s >= capacity();
	};

	default long enqueue(Iterator<IN> iter) {
		long c = 0;
		while (full())
			Concurrents.waitSleep(FULL_WAIT_MS);
		while (iter.hasNext()) {
			IN e = iter.next();
			if (null != e && enqueue(e)) c++;
		}
		return c;
	}

	default long enqueue(Iterable<IN> it) {
		return enqueue(it.iterator());
	}

	@SuppressWarnings("unchecked")
	default long enqueue(IN... e) {
		long c = 0;
		for (int i = 0; i < e.length; i++)
			if (e[i] != null && enqueue(e[i])) c++;
		return c;
	}

	List<OUT> dequeue(long batchSize);

	boolean isReadOrderly();

	boolean isWriteOrderly();

	void setReadOrderly(boolean orderly);

	void setWriteOrderly(boolean orderly);

	/* from interfaces */

	@Override
	default void close() {}

	default void gc() {}

	default void pump(Queue<OUT, ?, ?> dest, long batchSize, int parallelism) {
		ExecutorService ex = Concurrents.executor(parallelism, this.name(), dest.name());
		for (int i = 0; i < parallelism; i++)
			ex.submit(new Thread(new Runnable() {
				@Override
				public void run() {
					while (!empty())
						dest.enqueue(dequeue(batchSize));
				}
			}), this.name() + "-" + dest.name() + "-" + i);
	}
}
