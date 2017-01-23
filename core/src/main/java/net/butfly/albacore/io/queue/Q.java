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
public interface Q<I, O> extends Input<O>, Output<I> {
	static final long INFINITE_SIZE = -1;

	@Override
	long size();
	/* from interfaces */

	default <O2> Q<I, O2> then(Converter<O, O2> conv) {
		return thens(Collections.convAs(conv));
	}

	default <O2> Q<I, O2> thens(Converter<List<O>, List<O2>> conv) {
		return new QImpl<I, O2>(Q.this.name() + "Then", Q.this.capacity()) {
			@Override
			public boolean enqueue0(I d) {
				return Q.this.enqueue0(d);
			}

			@Override
			public O2 dequeue0() {
				O v = Q.this.dequeue0();
				if (null == v) return null;
				List<O2> l = conv.apply(Arrays.asList(v));
				if (null == l || l.isEmpty()) return null;
				return l.get(0);
			}

			@Override
			public List<O2> dequeue(long batchSize) {
				List<O> l = Q.this.dequeue(batchSize);
				return conv.apply(l);
			}

			@Override
			public long size() {
				return Q.this.size();
			}

			@Override
			public boolean empty() {
				return Q.this.empty();
			}

			@Override
			public boolean full() {
				return Q.this.full();
			}

			@Override
			public String toString() {
				return Q.this.getClass().getName() + "Then:" + name();
			}

			@Override
			public void close() {
				Q.this.close();
			}
		};
	}

	default <I0> Q<I0, O> prior(Converter<I0, I> conv) {
		return priors(Collections.convAs(conv));
	}

	default <I0> Q<I0, O> priors(Converter<List<I0>, List<I>> conv) {
		return new QImpl<I0, O>(Q.this.name() + "Prior", Q.this.capacity()) {
			@Override
			public boolean enqueue0(I0 item) {
				List<I> items = conv.apply(Arrays.asList(item));
				if (null == items || items.isEmpty()) return false;
				return Q.this.enqueue0(items.get(0));
			}

			@Override
			public long enqueue(List<I0> items) {
				return Q.this.enqueue(conv.apply(items));
			}

			@Override
			public O dequeue0() {
				return Q.this.dequeue0();
			}

			@Override
			public List<O> dequeue(long batchSize) {
				return Q.this.dequeue(batchSize);
			}

			@Override
			public long size() {
				return Q.this.size();
			}

			@Override
			public boolean empty() {
				return Q.this.empty();
			}

			@Override
			public boolean full() {
				return Q.this.full();
			}

			@Override
			public String toString() {
				return Q.this.getClass().getName() + "Prior:" + name();
			}

			@Override
			public void close() {
				Q.this.close();
			}
		};
	}
}
