package net.butfly.albacore.io.queue;

import java.util.function.Consumer;
import java.util.stream.Stream;

import net.butfly.albacore.io.IO;
import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.io.Streams;
import net.butfly.albacore.lambda.Converter;

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
public interface Queue<I, O> extends Input<O>, Output<I> {
	static final long INFINITE_SIZE = -1;

	@Override
	long size();

	/* from interfaces */

	@Override
	default <O1> Queue<I, O1> then(Converter<O, O1> conv) {
		Queue<I, O1> i = new Queue<I, O1>() {
			@Override
			public void dequeue(Consumer<Stream<O1>> using, long batchSize) {
				Queue.this.dequeue(s -> using.accept(s.map(conv)), batchSize);
			}

			@Override
			public long enqueue(Stream<I> items) {
				return Queue.this.enqueue(items);
			}

			@Override
			public long size() {
				return Queue.this.size();
			}
		};
		i.open();
		return i;
	}

	@Override
	default <O1> Queue<I, Stream<O1>> thens(Converter<Iterable<O>, Iterable<O1>> conv, int parallelism) {
		Queue<I, Stream<O1>> i = new Queue<I, Stream<O1>>() {
			@Override
			public void dequeue(Consumer<Stream<Stream<O1>>> using, long batchSize) {
				Queue.this.dequeue(s -> using.accept(Streams.batchMap(parallelism, s, conv)), batchSize);
			}

			@Override
			public long enqueue(Stream<I> items) {
				return Queue.this.enqueue(items);
			}

			@Override
			public long size() {
				return Queue.this.size();
			}
		};
		i.open();
		return i;
	}

	@Override
	default <I0> Queue<I0, O> prior(Converter<I0, I> conv) {
		Queue<I0, O> o = new Queue<I0, O>() {
			@Override
			public void dequeue(Consumer<Stream<O>> using, long batchSize) {
				Queue.this.dequeue(using, batchSize);
			}

			@Override
			public long enqueue(Stream<I0> items) {
				return Queue.this.enqueue(Streams.of(items.map(conv)));
			}

			@Override
			public long size() {
				return Queue.this.size();
			}
		};
		o.open();
		return o;
	}

	@Override
	default <I0> Queue<I0, O> priors(Converter<Iterable<I0>, Iterable<I>> conv, int parallelism) {
		Queue<I0, O> o = new Queue<I0, O>() {
			@Override
			public void dequeue(Consumer<Stream<O>> using, long batchSize) {
				Queue.this.dequeue(using, batchSize);
			}

			@Override
			public long enqueue(Stream<I0> items) {
				return IO.run(() -> Streams.batch(parallelism, items).mapToLong(s0 -> Queue.this.enqueue(Streams.of(conv.apply(
						(Iterable<I0>) () -> s0.iterator())))).sum());
			}

			@Override
			public long size() {
				return Queue.this.size();
			}
		};
		o.open();
		return o;
	}
}
