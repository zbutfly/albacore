package net.butfly.albacore.io.queue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.async.Concurrents;

public abstract class MapQImpl<K, I, O> extends QImpl<I, O> implements MapQ<K, I, O> {
	private static final long serialVersionUID = -1;
	private final Map<K, Q<I, O>> queues;
	private final Converter<I, K> keying;

	protected MapQImpl(String name, long capacity, Converter<I, K> keying) {
		super(name, capacity);
		this.queues = new HashMap<>();
		this.keying = keying;
	}

	protected MapQImpl(String name, long capacity, Converter<I, K> inkeying, Map<K, ? extends Q<I, O>> queues) {
		super(name, capacity);
		this.queues = new HashMap<>(queues);
		this.keying = inkeying;
	}

	@Override
	public Set<K> keys() {
		return queues.keySet();
	}

	@Override
	public Q<I, O> q(K key) {
		return queues.get(key);
	}

	@Override
	public long enqueue(Converter<I, K> keying, Iterator<I> iter) {
		while (full())
			Concurrents.waitSleep(FULL_WAIT_MS);
		return enqueueAll(i -> q(keying.apply(i)), iter);
	}

	private static <I, O> long enqueueAll(Converter<I, Q<I, O>> qing, Iterator<I> iter) {
		long count = 0;
		Iterator<I> remain = iter;
		while (!remain.hasNext())
			remain = enqueueRemain(qing, remain, count);
		return count;
	}

	private static <I, O> Iterator<I> enqueueRemain(Converter<I, Q<I, O>> qing, Iterator<I> remain, long... count) {
		List<I> r = new ArrayList<>();
		while (remain.hasNext()) {
			I e = remain.next();
			if (e != null) {
				Q<I, O> q = qing.apply(e);
				if (!q.full() && q.enqueue(e)) count[0]++;
				else r.add(e);
			}
		}
		return r.iterator();
	}

	/**********/
	@Override
	public boolean enqueue(I e) {
		return enqueue(keying.apply(e), Arrays.asList(e).iterator()) == 1;
	}

	@Override
	public long enqueue(Iterator<I> iter) {
		return enqueue(keying, iter);
	}

	@Override
	public O dequeue() {
		return MapQ.super.dequeue();
	}

	@Override
	public long size() {
		return MapQ.super.size();
	}
}
