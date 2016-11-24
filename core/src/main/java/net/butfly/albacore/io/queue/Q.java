package net.butfly.albacore.io.queue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import net.butfly.albacore.io.pump.Pump;
import net.butfly.albacore.io.pump.BasicPump;
import net.butfly.albacore.io.pump.MapPump;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;
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
public interface Q<I, O> extends AbstractQueue<I, O> {
	static final Logger logger = Logger.getLogger(QImpl.class);

	/**
	 * basic, none blocking writing.
	 * 
	 * @param d
	 * @return
	 */
	boolean enqueue(I d);

	/**
	 * basic, none blocking reading.
	 * 
	 * @return null on empty
	 */
	O dequeue();

	long size();

	long capacity();

	default boolean empty() {
		return size() <= 0;
	}

	default boolean full() {
		return size() >= capacity();
	}

	default long enqueue(Iterator<I> iter) {
		long c = 0;
		while (full())
			Concurrents.waitSleep(FULL_WAIT_MS);
		while (iter.hasNext()) {
			I e = iter.next();
			if (null != e && enqueue(e)) c++;
		}
		return c;
	}

	default long enqueue(Iterable<I> it) {
		return enqueue(it.iterator());
	}

	default long enqueue(@SuppressWarnings("unchecked") I... e) {
		return enqueue(Arrays.asList(e));
	}

	default List<O> dequeue(long batchSize) {
		List<O> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			O e = dequeue();
			if (null != e) batch.add(e);
			if (batch.size() == 0) Concurrents.waitSleep(EMPTY_WAIT_MS);
		} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		return batch;
	}

	/* from interfaces */

	@Override
	default void close() {}

	default Pump<O> pump(Q<O, ?> dest, int parallelism) {
		return new BasicPump<O>(this, dest, parallelism);
	}

	default <K> Pump<O> pump(MapQ<K, O, ?> dest, Converter<O, K> keying, int parallelism) {
		return new MapPump<K, O>(this, dest, keying, parallelism);
	}

	default <O2> Q<I, O2> then(Converter<O, O2> conv) {
		return new QImpl<I, O2>(null, Q.this.capacity()) {
			private static final long serialVersionUID = -5894142335125843377L;

			@Override
			public long size() {
				return Q.this.size();
			}

			@Override
			public boolean enqueue(I e) {
				return Q.this.enqueue(e);
			}

			@Override
			public O2 dequeue() {
				return conv.apply(Q.this.dequeue());
			}

			@Override
			public long enqueue(Iterable<I> it) {
				return Q.this.enqueue(it);
			}

			@Override
			public long enqueue(Iterator<I> iter) {
				return Q.this.enqueue(iter);
			}

			@SafeVarargs
			@Override
			public final long enqueue(I... e) {
				return Q.this.enqueue(e);
			}

			@Override
			public List<O2> dequeue(long batchSize) {
				return Collections.transform(Q.this.dequeue(batchSize), conv);
			}
		};
	}
}
