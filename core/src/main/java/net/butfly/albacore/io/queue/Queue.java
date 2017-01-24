package net.butfly.albacore.io.queue;

import java.util.Arrays;
import java.util.List;

import net.butfly.albacore.io.Input;
import net.butfly.albacore.io.Output;
import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;

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

	default <O1> Queue<I, O1> then(Converter<O, O1> conv) {
		return thens(Collections.convAs(conv));
	}

	default <O1> Queue<I, O1> thens(Converter<List<O>, List<O1>> conv) {
		return new QueueImpl<I, O1>(Queue.this.name() + "Then", Queue.this.capacity()) {
			@Override
			public O1 dequeue(boolean block) {
				return conv.apply(Arrays.asList(Queue.this.dequeue(block))).get(0);
			}

			@Override
			public boolean enqueue(I item, boolean block) {
				return Queue.this.enqueue(item, block);
			}

			@Override
			public boolean enqueue(I d) {
				return Queue.this.enqueue(d);
			}

			@Override
			public O1 dequeue() {
				O v = Queue.this.dequeue();
				if (null == v) return null;
				List<O1> l = conv.apply(Arrays.asList(v));
				if (null == l || l.isEmpty()) return null;
				return l.get(0);
			}

			@Override
			public List<O1> dequeue(long batchSize) {
				List<O> l = Queue.this.dequeue(batchSize);
				return conv.apply(l);
			}

			@Override
			public long size() {
				return Queue.this.size();
			}

			@Override
			public boolean empty() {
				return Queue.this.empty();
			}

			@Override
			public boolean full() {
				return Queue.this.full();
			}

			@Override
			public String toString() {
				return Queue.this.getClass().getName() + "Then:" + name();
			}

			@Override
			public void close() {
				Queue.this.close();
			}
		};
	}

	default <I0> Queue<I0, O> prior(Converter<I0, I> conv) {
		return priors(Collections.convAs(conv));
	}

	default <I0> Queue<I0, O> priors(Converter<List<I0>, List<I>> conv) {
		return new QueueImpl<I0, O>(Queue.this.name() + "Prior", Queue.this.capacity()) {

			@Override
			public O dequeue(boolean block) {
				return Queue.this.dequeue(block);
			}

			@Override
			public boolean enqueue(I0 item, boolean block) {
				return Queue.this.enqueue(conv.apply(Arrays.asList(item)).get(0), block);
			}

			@Override
			public boolean enqueue(I0 item) {
				List<I> items = conv.apply(Arrays.asList(item));
				if (null == items || items.isEmpty()) return false;
				return Queue.this.enqueue(items.get(0));
			}

			@Override
			public long enqueue(List<I0> items) {
				return Queue.this.enqueue(conv.apply(items));
			}

			@Override
			public O dequeue() {
				return Queue.this.dequeue();
			}

			@Override
			public List<O> dequeue(long batchSize) {
				return Queue.this.dequeue(batchSize);
			}

			@Override
			public long size() {
				return Queue.this.size();
			}

			@Override
			public boolean empty() {
				return Queue.this.empty();
			}

			@Override
			public boolean full() {
				return Queue.this.full();
			}

			@Override
			public String toString() {
				return Queue.this.getClass().getName() + "Prior:" + name();
			}

			@Override
			public void close() {
				Queue.this.close();
			}
		};
	}
}
