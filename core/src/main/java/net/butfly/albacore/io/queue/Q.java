package net.butfly.albacore.io.queue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.butfly.albacore.io.Openable;
import net.butfly.albacore.io.pump.BasicPump;
import net.butfly.albacore.io.pump.FanoutPump;
import net.butfly.albacore.io.pump.Pump;
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
public interface Q<I, O> extends Openable, Serializable {
	static final Logger logger = Logger.getLogger(Q.class);
	static final long INFINITE_SIZE = -1;
	static final long FULL_WAIT_MS = 500;
	static final long EMPTY_WAIT_MS = 500;

	@Override
	String name();

	long size();

	long capacity();

	default boolean empty() {
		return size() <= 0;
	}

	default boolean full() {
		return size() >= capacity();
	}

	/**
	 * basic, none blocking reading.
	 * 
	 * @return null on empty
	 */
	O dequeue0();

	default List<O> dequeue(long batchSize) {
		List<O> batch = new ArrayList<>();
		long prev;
		do {
			prev = batch.size();
			O e = dequeue0();
			if (null != e) batch.add(e);
			if (batch.size() == 0) Concurrents.waitSleep(EMPTY_WAIT_MS);
		} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		return batch;
	}

	/**
	 * basic, none blocking writing.
	 * 
	 * @param d
	 * @return
	 */
	boolean enqueue0(I d);

	default long enqueue(List<I> items) {
		long c = 0;
		while (full())
			Concurrents.waitSleep(FULL_WAIT_MS);
		for (I e : items)
			if (null != e) {
				if (enqueue0(e)) c++;
				else logger.warn("Q enqueue failure though not full before, item maybe lost");
			}
		return c;
	}

	@SuppressWarnings("unchecked")
	default long enqueue(I... e) {
		return enqueue(Arrays.asList(e));
	}

	/* from interfaces */

	default Pump pump(int parallelism, Q<O, ?> dest) {
		return new BasicPump(this, dest, parallelism);
	}

	@SuppressWarnings("unchecked")
	default Pump pump(int parallelism, Q<O, ?>... dests) {
		return new FanoutPump(this, parallelism, Arrays.asList(dests));
	}

	default Pump pump(int parallelism, List<? extends Q<O, ?>> dests) {
		return new FanoutPump(this, parallelism, dests);
	}

	default <O2> Q<I, O2> then(Converter<O, O2> conv) {
		return thens(Collections.convAs(conv));
	}

	default <O2> Q<I, O2> thens(Converter<List<O>, List<O2>> conv) {
		return new QImpl<I, O2>(Q.this.name() + "-Then", Q.this.capacity()) {
			private static final long serialVersionUID = -5894142335125843377L;

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
			public void closing() {}
		};
	}

	default <I0> Q<I0, O> prior0(Converter<I0, I> conv) {
		return priors(Collections.convAs(conv));
	}

	default <I0> Q<I0, O> priors(Converter<List<I0>, List<I>> conv) {
		return new QImpl<I0, O>(Q.this.name() + "-Prior", Q.this.capacity()) {
			private static final long serialVersionUID = -2063675795097988806L;

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
			public void closing() {}
		};
	}

}
