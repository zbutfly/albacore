package net.butfly.albacore.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import net.butfly.albacore.lambda.Converter;

class MapQueueImpl<K, E, Q extends AbstractQueue<E>> extends AbstractQueue<E> implements MapQueue<K, E> {
	private static final long serialVersionUID = 3551659378491886759L;
	private final Map<K, Q> queues;
	final Converter<K, Q> constructor;
	final Converter<E, K> keying;

	private final ReentrantReadWriteLock locker;

	protected MapQueueImpl(String name, Converter<E, K> keying, Converter<K, Q> constructor, long capacity) {
		super(name, capacity);
		queues = new ConcurrentHashMap<>();
		this.constructor = constructor;
		this.keying = keying;
		locker = new ReentrantReadWriteLock();
	}

	@Override
	public final boolean empty(K key) {
		for (Q q : queues.values())
			if (!q.empty()) return false;
		return true;
	}

	@Override
	protected final E dequeueRaw() {
		List<E> l = dequeue(1);
		return l.isEmpty() ? null : l.get(0);
	}

	@Override
	public final E dequeue(K key) {
		return q(key).dequeue();
	}

	@Override
	@SafeVarargs
	public final List<E> dequeue(long batchSize, K... key) {
		List<E> batch = new ArrayList<>();
		Iterable<K> ks = key == null || key.length == 0 ? queues.keySet() : Arrays.asList(key);
		int prev;
		do {
			prev = batch.size();
			for (K k : ks) {
				Q q = q(k);
				E e = q.dequeue();
				if (null != e) {
					batch.add(e);
					stats(e, false);
				}
				if (q.empty()) q.gc();
			}
		} while (batch.size() < batchSize && prev != batch.size());
		return batch;
	}

	@Override
	protected boolean enqueueRaw(E e) {
		return enqueue(keying.apply(e), e);
	}

	@Override
	public final boolean enqueue(K key, E e) {
		return q(key).enqueue(e);
	}

	@SafeVarargs
	@Override
	public final long enqueue(Converter<E, K> key, E... e) {
		List<E> remain = new ArrayList<>(Arrays.asList(e));
		AtomicLong c = new AtomicLong(0);
		while (!remain.isEmpty())
			remain = enqueue(key, remain, c);
		return c.get();
	}

	private final List<E> enqueue(Converter<E, K> key, List<E> l, AtomicLong c) {
		List<E> remain = new ArrayList<>();
		for (E ee : l)
			if (ee != null) {
				Q q = q(key.apply(ee));
				if (!q.full()) {
					q.enqueueRaw(ee);
					c.getAndIncrement();
				} else remain.add(ee);
			}
		return remain;
	}

	@Override
	public final long enqueue(Converter<E, K> key, Iterable<E> it) {
		return enqueue(key, it.iterator());
	}

	@Override
	public final long enqueue(Converter<E, K> key, Iterator<E> iter) {
		long c = 0;
		while (iter.hasNext()) {
			E e = iter.next();
			if (null != e && enqueue(key.apply(e), e)) c++;
		}
		return c;
	}

	protected final Q q(K key) {
		locker.writeLock().lock();
		try {
			if (!queues.containsKey(key)) queues.put(key, constructor.apply(key));
			return queues.get(key);
		} finally {
			locker.writeLock().unlock();
		}
	}

	@Override
	public final long size() {
		long s = 0;
		for (Q q : queues.values())
			s += q.size();
		return s;
	}

	@Override
	public final long size(K key) {
		return q(key).size();
	}
}
