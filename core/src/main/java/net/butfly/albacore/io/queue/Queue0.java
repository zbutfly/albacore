package net.butfly.albacore.io.queue;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.io.utils.Its;
import net.butfly.albacore.io.utils.Streams;

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
 */
public interface Queue0<I, O> extends Input<O>, Output<I> {
	static final long INFINITE_SIZE = -1;

	@Override
	long size();

	/* from interfaces */

	@Override
	default <O1> Queue0<I, O1> then(Function<O, O1> conv) {
		Queue0<I, O1> i = new Queue0<I, O1>() {
			@Override
			public long dequeue(Function<Stream<O1>, Long> using, long batchSize) {
				return Queue0.this.dequeue(s -> using.apply(s.map(conv)), batchSize);
			}

			@Override
			public long enqueue(Stream<I> items) {
				return Queue0.this.enqueue(items);
			}

			@Override
			public long size() {
				return Queue0.this.size();
			}
		};
		i.open();
		return i;
	}

	@Override
	default <O1> Queue0<I, O1> thens(Function<Iterable<O>, Iterable<O1>> conv, int parallelism) {
		Queue0<I, O1> i = new Queue0<I, O1>() {
			@Override
			public long dequeue(Function<Stream<O1>, Long> using, long batchSize) {
				return Queue0.this.dequeue(s -> {
					AtomicLong c = new AtomicLong();
					Streams.spatialMap(s, parallelism, t -> conv.apply(() -> Its.it(t)).spliterator()).forEach(s1 -> c.addAndGet(using
							.apply(s1)));
					return c.get();
				}, batchSize);
			}

			@Override
			public long enqueue(Stream<I> items) {
				return Queue0.this.enqueue(items);
			}

			@Override
			public long size() {
				return Queue0.this.size();
			}
		};
		i.open();
		return i;
	}

	@Override
	default <I0> Queue0<I0, O> prior(Function<I0, I> conv) {
		Queue0<I0, O> o = new Queue0<I0, O>() {
			@Override
			public long dequeue(Function<Stream<O>, Long> using, long batchSize) {
				return Queue0.this.dequeue(using, batchSize);
			}

			@Override
			public long enqueue(Stream<I0> items) {
				return Queue0.this.enqueue(Streams.of(items.map(conv)));
			}

			@Override
			public long size() {
				return Queue0.this.size();
			}
		};
		o.open();
		return o;
	}

	@Override
	default <I0> Queue0<I0, O> priors(Function<Iterable<I0>, Iterable<I>> conv, int parallelism) {
		Queue0<I0, O> o = new Queue0<I0, O>() {
			@Override
			public long dequeue(Function<Stream<O>, Long> using, long batchSize) {
				return Queue0.this.dequeue(using, batchSize);
			}

			@Override
			public long enqueue(Stream<I0> items) {
				return eachs(Streams.spatial(items, parallelism).values(), s0 -> Queue0.this.enqueue(Streams.of(conv.apply(
						(Iterable<I0>) () -> Its.it(s0)))), Streams.LONG_SUM);
			}

			@Override
			public long size() {
				return Queue0.this.size();
			}
		};
		o.open();
		return o;
	}
}
