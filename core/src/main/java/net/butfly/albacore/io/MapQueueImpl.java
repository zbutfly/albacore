package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.async.Concurrents;
import scala.Tuple2;

public class MapQueueImpl<K, IN, OUT, DATA, Q extends AbstractQueue<IN, OUT, DATA>> extends AbstractQueue<IN, OUT, DATA> implements
		MapQueue<K, IN, OUT, DATA> {
	private static final long serialVersionUID = 3551659378491886759L;
	private final Map<K, Q> queues;
	protected final Converter<IN, K> keying;
	protected final Tuple2<Converter<DATA, IN>, Converter<OUT, DATA>> unconv;

	public MapQueueImpl(String name, long capacity, Converter<IN, K> keying, //
			Converter<IN, DATA> convin, Converter<DATA, OUT> convout, //
			Converter<DATA, IN> unconvin, Converter<OUT, DATA> unconvout) {
		super(name, capacity, convin, convout);
		this.queues = new HashMap<>();
		this.keying = keying;
		this.unconv = new Tuple2<>(unconvin, unconvout);
	}

	public final MapQueueImpl<K, IN, OUT, DATA, Q> initialize(Map<K, Q> queues) {
		this.queues.putAll(queues);
		return this;
	}

	@Override
	public final boolean empty(K key) {
		for (Q q : queues.values())
			if (!q.empty()) return false;
		return true;
	}

	@Override
	protected final DATA dequeueRaw() {
		throw new UnsupportedOperationException();
	}

	@Override
	public final OUT dequeue(K key) {
		return queues.get(key).dequeue();
	}

	@Override
	public List<OUT> dequeue(long batchSize) {
		return dequeue(batchSize, (K[]) null);
	}

	@Override
	@SafeVarargs
	public final List<OUT> dequeue(long batchSize, K... key) {
		List<OUT> batch = new ArrayList<>();
		Iterable<K> ks = key == null || key.length == 0 ? queues.keySet() : Arrays.asList(key);
		int prev;
		do {
			prev = batch.size();
			for (K k : ks) {
				Q q = queues.get(k);
				DATA e = q.dequeueRaw();
				if (null != e) {
					batch.add(conv._2.apply(e));
					if (q.empty()) q.gc();
				}
			}
			if (batch.size() == 0) Concurrents.waitSleep(EMPTY_WAIT_MS);
		} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		return batch;
	}

	@Override
	protected boolean enqueueRaw(DATA e) {
		throw new UnsupportedOperationException();
	}

	@Override
	public final boolean enqueue(K key, IN e) {
		return queues.get(key).enqueue(e);
	}

	@Override
	public final boolean enqueue(IN e) {
		while (full())
			if (!Concurrents.waitSleep(FULL_WAIT_MS)) logger.warn("Wait for full interrupted");
		return enqueue(keying.apply(e), e);
	}

	@SafeVarargs
	@Override
	public final long enqueue(IN... e) {
		return enqueue(keying, e);
	}

	@SafeVarargs
	@Override
	public final long enqueue(Converter<IN, K> key, IN... e) {
		return enqueueList(new ArrayList<>(Arrays.asList(e)), key);
	}

	@Override
	public final long enqueue(Converter<IN, K> key, Iterator<IN> iter) {
		List<IN> remain = new ArrayList<>();
		while (iter.hasNext())
			remain.add(iter.next());
		return enqueueList(remain, key);
	}

	private long enqueueList(List<IN> remain, Converter<IN, K> key) {
		while (full())
			Concurrents.waitSleep(FULL_WAIT_MS);
		long c = 0;
		while (!remain.isEmpty())
			remain = enqueueRemain(key, remain, c);
		return c;
	}

	private final List<IN> enqueueRemain(Converter<IN, K> key, List<IN> l, long... c) {
		List<IN> remain = new ArrayList<>();
		for (IN ee : l)
			if (ee != null) {
				Q q = queues.get(keying.apply(ee));
				if (!q.full() && q.enqueueRaw(conv._1.apply(ee))) c[0]++;
				else remain.add(ee);
			}
		return remain;
	}

	@Override
	public long size() {
		long s = 0;
		for (Q q : queues.values())
			s += q.size();
		return s;
	}

	@Override
	public final long size(K key) {
		return queues.get(key).size();
	}
}
