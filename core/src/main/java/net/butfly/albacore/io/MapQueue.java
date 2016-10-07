package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.async.Concurrents;
import scala.Tuple2;

public class MapQueue<K, I, O, D, Q extends AbstractQueue<I, O, D>> extends AbstractQueue<I, O, D> implements
		Queue<I, O, D> {
	private static final long serialVersionUID = 3551659378491886759L;
	private final Map<K, Q> queues;
	protected final Converter<I, K> keying;
	protected final Tuple2<Converter<D, I>, Converter<O, D>> unconv;

	public MapQueue(String name, long capacity, Converter<I, K> keying, //
			Converter<I, D> convin, Converter<D, O> convout, //
			Converter<D, I> unconvin, Converter<O, D> unconvout) {
		super(name, capacity, convin, convout);
		this.queues = new HashMap<>();
		this.keying = keying;
		this.unconv = new Tuple2<>(unconvin, unconvout);
	}

	public final MapQueue<K, I, O, D, Q> initialize(Map<K, Q> queues) {
		this.queues.putAll(queues);
		return this;
	}

	public final boolean empty(K key) {
		for (Q q : queues.values())
			if (!q.empty()) return false;
		return true;
	}

	@Override
	protected final D dequeueRaw() {
		throw new UnsupportedOperationException();
	}

	public final O dequeue(K key) {
		return queues.get(key).dequeue();
	}

	@Override
	public List<O> dequeue(long batchSize) {
		return dequeue(batchSize, (K[]) null);
	}

	@SafeVarargs
	public final List<O> dequeue(long batchSize, K... key) {
		List<O> batch = new ArrayList<>();
		Iterable<K> ks = key == null || key.length == 0 ? queues.keySet() : Arrays.asList(key);
		int prev;
		do {
			prev = batch.size();
			for (K k : ks) {
				Q q = queues.get(k);
				D e = q.dequeueRaw();
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
	protected boolean enqueueRaw(D e) {
		throw new UnsupportedOperationException();
	}

	public final boolean enqueue(K key, I e) {
		return queues.get(key).enqueue(e);
	}

	public long enqueue(Converter<I, K> key, Iterable<I> it) {
		return enqueue(key, it.iterator());
	}

	@Override
	public final boolean enqueue(I e) {
		while (full())
			if (!Concurrents.waitSleep(FULL_WAIT_MS)) logger.warn("Wait for full interrupted");
		return enqueue(keying.apply(e), e);
	}

	@SafeVarargs
	@Override
	public final long enqueue(I... e) {
		return enqueue(keying, e);
	}

	@SafeVarargs
	public final long enqueue(Converter<I, K> key, I... e) {
		return enqueueList(new ArrayList<>(Arrays.asList(e)), key);
	}

	public final long enqueue(Converter<I, K> key, Iterator<I> iter) {
		List<I> remain = new ArrayList<>();
		while (iter.hasNext())
			remain.add(iter.next());
		return enqueueList(remain, key);
	}

	private long enqueueList(List<I> remain, Converter<I, K> key) {
		while (full())
			Concurrents.waitSleep(FULL_WAIT_MS);
		long c = 0;
		while (!remain.isEmpty())
			remain = enqueueRemain(key, remain, c);
		return c;
	}

	private final List<I> enqueueRemain(Converter<I, K> key, List<I> l, long... c) {
		List<I> remain = new ArrayList<>();
		for (I ee : l)
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

	public final long size(K key) {
		return queues.get(key).size();
	}

	public void pump(MapQueue<K, O, ?, ?, ?> dest, long batchSize, int parallelism) {
		ExecutorService ex = Concurrents.executor(parallelism, this.name(), dest.name());
		for (int i = 0; i < parallelism; i++)
			ex.submit(new Thread(new Runnable() {
				@Override
				public void run() {

				}
			}), this.name() + "-" + dest.name() + "-" + i);
	}
}
