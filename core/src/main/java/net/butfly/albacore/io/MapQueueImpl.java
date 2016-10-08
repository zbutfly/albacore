package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.async.Concurrents;

public abstract class MapQueueImpl<K, I, O, D> extends QueueImpl<I, O, D> implements MapQueue<K, I, O> {
	private static final long serialVersionUID = -1;
	private final Map<K, Queue<I, O>> queues;

	public MapQueueImpl(String name, long capacity) {
		super(name, capacity);
		this.queues = new HashMap<>();
	}

	abstract protected K keying(I e);

	@Override
	public final MapQueue<K, I, O> initialize(Map<K, ? extends Queue<I, O>> queues) {
		this.queues.putAll(queues);
		return this;
	}

	public final boolean empty(K key) {
		for (Queue<I, O> q : queues.values())
			if (!q.empty()) return false;
		return true;
	}

	public final O dequeue(K key) {
		return queues.get(key).dequeue();
	}

	@Override
	public final List<O> dequeue(long batchSize) {
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
				QueueImpl<I, O, ?> q = q(k);
				O e = q.dequeueRaw();
				if (null != e) {
					batch.add(e);
					if (q.empty()) q.gc();
				}
			}
			if (batch.size() == 0) Concurrents.waitSleep(EMPTY_WAIT_MS);
		} while (batch.size() < batchSize && (prev != batch.size() || batch.size() == 0));
		return batch;
	}

	@Override
	@Deprecated
	protected final boolean enqueueRaw(I e) {
		return false;
	}

	@Override
	@Deprecated
	protected final O dequeueRaw() {
		return null;
	}

	public final boolean enqueue(K key, I e) {
		return queues.get(key).enqueue(e);
	}

	public final long enqueue(Converter<I, K> key, Iterable<I> it) {
		return enqueue(key, it.iterator());
	}

	@Override
	public final boolean enqueue(I e) {
		while (full())
			if (!Concurrents.waitSleep(FULL_WAIT_MS)) logger.warn("Wait for full interrupted");
		return enqueue(keying(e), e);
	}

	@SafeVarargs
	@Override
	public final long enqueue(I... e) {
		return enqueue(i -> keying(i), e);
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
		for (I e : l)
			if (e != null) {
				QueueImpl<I, O, ?> q = q(keying(e));
				if (!q.full() && q.enqueueRaw(e)) c[0]++;
				else remain.add(e);
			}
		return remain;
	}

	@Override
	public long size() {
		long s = 0;
		for (Queue<I, O> q : queues.values())
			s += q.size();
		return s;
	}

	public final long size(K key) {
		return queues.get(key).size();
	}

	@Override
	public void close() {
		super.close();
		for (Queue<I, O> q : queues.values())
			q.close();
	}

	@SuppressWarnings("unchecked")
	private QueueImpl<I, O, ?> q(K key) {
		return (QueueImpl<I, O, ?>) queues.get(key);
	}
}
