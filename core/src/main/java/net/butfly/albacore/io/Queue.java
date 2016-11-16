package net.butfly.albacore.io;

import java.util.Iterator;
import java.util.List;

import net.butfly.albacore.io.pump.Pump;
import net.butfly.albacore.io.pump.PumpBase;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.lambda.Supplier;
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

	long enqueue(@SuppressWarnings("unchecked") I... e);

	List<O> dequeue(long batchSize);

	/* from interfaces */

	@Override
	default void close() {}

	default void gc() {}

	default Pump<O> pump(Queue<O, ?> dest, int parallelism) {
		return pump(dest, parallelism, () -> this.empty());
	}

	default Pump<O> pump(Queue<O, ?> dest, int parallelism, Supplier<Boolean> stopping) {
		return new PumpBase<O>(this, dest, parallelism) {
			private static final long serialVersionUID = 1664281939439030486L;

			@Override
			public boolean stopped() {
				return super.stopped() && stopping.get();
			}
		};
	}

	default <O2> Queue<I, O2> then(Converter<O, O2> conv) {
		return new QueueImpl<I, O2>(null, Queue.this.capacity()) {
			private static final long serialVersionUID = -5894142335125843377L;

			@Override
			public long size() {
				return Queue.this.size();
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
				return Queue.this.enqueue(e);
			}

			@Override
			public long enqueue(Iterable<I> it) {
				return Queue.this.enqueue(it);
			}

			@Override
			public long enqueue(Iterator<I> iter) {
				return Queue.this.enqueue(iter);
			}

			@SafeVarargs
			@Override
			public final long enqueue(I... e) {
				return Queue.this.enqueue(e);
			}

			@Override
			public O2 dequeue() {
				return conv.apply(Queue.this.dequeue());
			}

			@Override
			public List<O2> dequeue(long batchSize) {
				return Collections.transform(Queue.this.dequeue(batchSize), conv);
			}
		};
	}
}
