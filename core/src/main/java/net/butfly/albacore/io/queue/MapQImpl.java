package net.butfly.albacore.io.queue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.butfly.albacore.lambda.Converter;
import net.butfly.albacore.utils.Collections;

public abstract class MapQImpl<K, I, O> extends QImpl<I, O> implements MapQ<K, I, O> {
	private static final long serialVersionUID = -1;
	private final Map<K, ? extends QImpl<I, O>> queues;
	private final Converter<I, K> keying;

	protected MapQImpl(String name, long capacity, Converter<I, K> keying) {
		super(name, capacity);
		this.queues = new HashMap<>();
		this.keying = keying;
	}

	protected MapQImpl(String name, long capacity, Converter<I, K> inkeying, Map<K, ? extends QImpl<I, O>> queues) {
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
	public void closing() {
		for (K k : keys()) {
			Q<I, O> q = q(k);
			if (null != q) q.close();
		}
	}

	static <I, O> long enqueueAll(Converter<I, Q<I, O>> qing, List<I> items) {
		long count = 0;
		List<I> remain = items;
		while (!remain.isEmpty())
			remain = enqueueRemain(qing, remain, count);
		return count;
	}

	static <I, O> List<I> enqueueRemain(Converter<I, Q<I, O>> qing, List<I> remain, long... count) {
		List<I> r = new ArrayList<>();
		for (I e : remain)
			if (e != null) {
				Q<I, O> q = qing.apply(e);
				if (null != q && !q.full() && q.enqueue0(e)) count[0]++;
				else r.add(e);
			}
		return r;
	}

	/**********/
	@Override
	public O dequeue0() {
		for (K k : Collections.disorderize(keys())) {
			Q<I, O> q = q(k);
			if (null == q) continue;
			O o = q.dequeue0();
			if (null != o) return o;
		}
		return null;
	}

	@Override
	public boolean enqueue0(I e) {
		return enqueue0(keying.apply(e), e);
	}

	@Override
	public long size() {
		return MapQ.super.size();
	}
}
