package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.List;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;
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
public interface Queue<I, O> extends AbstractQueue<I, O> {
	static final Logger logger = Logger.getLogger(QueueImpl.class);
	static final long INFINITE_SIZE = -1;
	static final long FULL_WAIT_MS = 500;
	static final long EMPTY_WAIT_MS = 500;

	/* from Queue */

	boolean enqueue(I e);

	O dequeue();

	/* for rich features */

	long size();

	long capacity();

	boolean empty();

	boolean full();

	long enqueue(Iterator<I> iter);

	long enqueue(Iterable<I> it);

	@SuppressWarnings("unchecked")
	long enqueue(I... e);

	List<O> dequeue(long batchSize);

	/* from interfaces */

	@Override
	default void close() {}

	default void gc() {}

	default DirectPump<O> pump(Queue<O, ?> dest, long batchSize, int parallelism) {
		DirectPump<O> p = new DirectPump<O>(this, dest, parallelism);
		p.submit(() -> dest.enqueue(dequeue(batchSize)) <= 0, parallelism);
		return p;
	}

	default <O2> Queue<I, O2> then(Converter<O, O2> conv) {
		@SuppressWarnings("resource")
		final Queue<I, O> main = this;
		return new QueueImpl<I, O2, O>(null, main.capacity()) {
			private static final long serialVersionUID = -5894142335125843377L;

			@Override
			public long size() {
				return main.size();
			}

			@Override
			protected boolean enqueueRaw(I d) {
				return false;
			}

			@Override
			protected O2 dequeueRaw() {
				return null;
			}

			@Override
			public boolean enqueue(I e) {
				return main.enqueue(e);
			}

			@Override
			public long enqueue(Iterable<I> it) {
				return main.enqueue(it);
			}

			@Override
			public long enqueue(Iterator<I> iter) {
				return main.enqueue(iter);
			}

			@SafeVarargs
			@Override
			public final long enqueue(I... e) {
				return main.enqueue(e);
			}

			@Override
			public O2 dequeue() {
				return conv.apply(main.dequeue());
			}

			@Override
			public List<O2> dequeue(long batchSize) {
				return Collections.transform(main.dequeue(batchSize), conv);
			}
		};
	}
}
